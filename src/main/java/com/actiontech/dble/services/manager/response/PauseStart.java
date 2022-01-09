/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.handler.PacketResult;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.PauseShardingNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PauseStart {
    private static final Pattern PATTERN_FOR_PAUSE = Pattern.compile("^\\s*pause\\s*@@shardingNode\\s*=\\s*'([a-zA-Z_0-9,]+)'\\s*and\\s*timeout\\s*=\\s*([0-9]+)\\s*(,\\s*queue\\s*=\\s*([0-9]+)){0,1}\\s*(,\\s*wait_limit\\s*=\\s*([0-9]+)){0,1}\\s*$", Pattern.CASE_INSENSITIVE);
    private static final OkPacket OK = new OkPacket();
    private static final int DEFAULT_CONNECTION_TIME_OUT = 120000;
    private static final int DEFAULT_QUEUE_LIMIT = 200;
    private static final Logger LOGGER = LoggerFactory.getLogger(PauseStart.class);

    private PauseStart() {
    }

    static {
        OK.setPacketId(1);
        OK.setAffectedRows(1L);
        OK.setServerStatus(2);
    }

    public static void execute(final ManagerService c, final String sql) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                pause(c, sql);
            }
        });

    }

    public static void pause(ManagerService service, String sql) {
        LOGGER.info("pause start from command");
        Matcher ma = PATTERN_FOR_PAUSE.matcher(sql);
        PacketResult packetResult = new PacketResult();
        try {
            if (!ma.matches()) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("The sql did not match pause @@shardingNode ='dn......' and timeout = ([0-9]+)");
                packetResult.setErrorCode(ErrorCode.ER_UNKNOWN_ERROR);
                return;
            }
            String shardingNode = ma.group(1);
            int connectionTimeOut = ma.group(6) == null ? DEFAULT_CONNECTION_TIME_OUT : Integer.parseInt(ma.group(6)) * 1000;
            int queueLimit = ma.group(4) == null ? DEFAULT_QUEUE_LIMIT : Integer.parseInt(ma.group(4));
            Set<String> shardingNodes = new HashSet<>(Arrays.asList(shardingNode.split(",")));
            //check shardingNode
            for (String singleDn : shardingNodes) {
                if (DbleServer.getInstance().getConfig().getShardingNodes().get(singleDn) == null) {
                    packetResult.setSuccess(false);
                    packetResult.setErrorMsg("ShardingNode " + singleDn + " did not exists");
                    packetResult.setErrorCode(ErrorCode.ER_UNKNOWN_ERROR);
                    return;
                }
            }

            if (!PauseShardingNodeManager.getInstance().getDistributeLock()) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Other node is doing pause operation concurrently");
                packetResult.setErrorCode(ErrorCode.ER_UNKNOWN_ERROR);
                return;
            }


            try {
                try {
                    //clusterPauseNotice
                    PauseShardingNodeManager.getInstance().clusterPauseNotice(shardingNode, connectionTimeOut, queueLimit);
                } catch (Exception e) {
                    LOGGER.warn("pause failed", e);
                    packetResult.setSuccess(false);
                    packetResult.setErrorMsg(e.getMessage());
                    packetResult.setErrorCode(ErrorCode.ER_YES);
                    return;
                }


                PauseShardingNodeManager.getInstance().startPausing(connectionTimeOut, shardingNodes, shardingNode, queueLimit);

                //self pause the shardingNode
                long timeOut = Long.parseLong(ma.group(2)) * 1000;
                long beginTime = System.currentTimeMillis();
                boolean recycleFinish = waitForSelfPause(beginTime, timeOut, shardingNodes);

                LOGGER.info("wait finished " + recycleFinish);
                if (!recycleFinish) {
                    packetResult.setSuccess(false);
                    packetResult.setErrorCode(ErrorCode.ER_YES);
                    if (PauseShardingNodeManager.getInstance().tryResume()) {
                        try {
                            PauseShardingNodeManager.getInstance().resumeCluster();
                        } catch (Exception e) {
                            LOGGER.warn("resume cause error", e);
                        }

                        packetResult.setErrorMsg("The backend connection recycle failure, try it later");
                    } else {
                        packetResult.setErrorMsg("Pause resume when recycle connection, pause revert");
                    }
                } else {
                    try {
                        if (PauseShardingNodeManager.getInstance().waitForCluster(beginTime, timeOut, packetResult)) {
                            LOGGER.info("call pause success");
                        }
                    } catch (Exception e) {
                        LOGGER.warn("wait for other node failed.", e);
                        packetResult.setSuccess(false);
                        packetResult.setErrorMsg(e.getMessage());
                        packetResult.setErrorCode(ErrorCode.ER_YES);
                    }
                }
            } catch (MySQLOutPutException e) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg(e.getMessage());
                packetResult.setErrorCode(ErrorCode.ER_YES);
            } catch (Exception e) {
                packetResult.setSuccess(false);
                packetResult.setErrorMsg("Pause operation cause error: " + e.getMessage());
                packetResult.setErrorCode(ErrorCode.ER_YES);
            } finally {
                PauseShardingNodeManager.getInstance().releaseDistributeLock();
            }
        } finally {
            writePacket(packetResult.isSuccess(), service, packetResult.getErrorMsg(), packetResult.getErrorCode());
        }
    }


    private static void writePacket(boolean isSuccess, ManagerService service, String errorMsg, int errorCode) {
        if (isSuccess) {
            OK.write(service.getConnection());
        } else {
            service.writeErrMessage(errorCode, errorMsg);
        }
    }

    public static boolean waitForSelfPause(long beginTime, long timeOut, Set<String> shardingNodes) {
        boolean recycleFinish = false;
        while ((System.currentTimeMillis() - beginTime < timeOut) && PauseShardingNodeManager.getInstance().getIsPausing().get()) {
            boolean nextTurn = false;
            for (IOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                for (Map.Entry<Long, FrontendConnection> entry : processor.getFrontends().entrySet()) {
                    if (!entry.getValue().isManager()) {
                        ShardingService shardingService = (ShardingService) entry.getValue().getService();
                        for (Map.Entry<RouteResultsetNode, BackendConnection> conEntry : shardingService.getSession2().getTargetMap().entrySet()) {
                            if (shardingNodes.contains(conEntry.getKey().getName())) {
                                nextTurn = true;
                                break;
                            }
                        }
                        if (nextTurn) {
                            break;
                        }
                    }
                }
                if (nextTurn) {
                    break;
                }
            }
            if (!nextTurn) {
                recycleFinish = true;
                break;
            } else {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            }
        }
        return recycleFinish;
    }
}
