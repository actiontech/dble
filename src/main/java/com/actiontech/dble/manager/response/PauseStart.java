/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.singleton.PauseDatanodeManager;
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

    public static void execute(final ManagerConnection c, final String sql) {
        DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
            @Override
            public void run() {
                pause(c, sql);
            }
        });

    }


    public static void pause(ManagerConnection c, String sql) {

        Matcher ma = PATTERN_FOR_PAUSE.matcher(sql);
        if (!ma.matches()) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "The sql did not match pause @@shardingNode ='dn......' and timeout = ([0-9]+)");
            return;
        }
        String shardingNode = ma.group(1);
        int connectionTimeOut = ma.group(6) == null ? DEFAULT_CONNECTION_TIME_OUT : Integer.parseInt(ma.group(6)) * 1000;
        int queueLimit = ma.group(4) == null ? DEFAULT_QUEUE_LIMIT : Integer.parseInt(ma.group(4));
        Set<String> shardingNodes = new HashSet<>(Arrays.asList(shardingNode.split(",")));
        //check dataNodes
        for (String singleDn : shardingNodes) {
            if (DbleServer.getInstance().getConfig().getShardingNodes().get(singleDn) == null) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "ShardingNode " + singleDn + " did not exists");
                return;
            }
        }


        //clusterPauseNotic
        if (!PauseDatanodeManager.getInstance().clusterPauseNotic(shardingNode, connectionTimeOut, queueLimit)) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "Other node in cluster is pausing");
            return;
        }


        if (!PauseDatanodeManager.getInstance().startPausing(connectionTimeOut, shardingNodes, queueLimit)) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "Some shardingNodes is paused, please resume first");
            return;
        }


        //self pause the dataNode
        long timeOut = Long.parseLong(ma.group(2)) * 1000;
        long beginTime = System.currentTimeMillis();
        boolean recycleFinish = waitForSelfPause(beginTime, timeOut, shardingNodes);

        LOGGER.info("wait finished " + recycleFinish);
        if (!recycleFinish) {
            if (PauseDatanodeManager.getInstance().tryResume()) {
                try {
                    PauseDatanodeManager.getInstance().resumeCluster();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage());
                }
                c.writeErrMessage(1003, "The backend connection recycle failure,try it later");
            } else {
                c.writeErrMessage(1003, "Pause resume when recycle connection ,pause revert");
            }

        } else {
            try {
                if (PauseDatanodeManager.getInstance().waitForCluster(c, beginTime, timeOut)) {
                    OK.write(c);
                }
            } catch (Exception e) {
                LOGGER.warn(e.getMessage());
                c.writeErrMessage(1003, e.getMessage());
            }
        }
    }


    private static boolean waitForSelfPause(long beginTime, long timeOut, Set<String> shardingNodes) {
        boolean recycleFinish = false;
        while ((System.currentTimeMillis() - beginTime < timeOut) && PauseDatanodeManager.getInstance().getIsPausing().get()) {
            boolean nextTurn = false;
            for (NIOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                for (Map.Entry<Long, FrontendConnection> entry : processor.getFrontends().entrySet()) {
                    if ((entry.getValue() instanceof ServerConnection)) {
                        ServerConnection sconnection = (ServerConnection) entry.getValue();
                        for (Map.Entry<RouteResultsetNode, BackendConnection> conEntry : sconnection.getSession2().getTargetMap().entrySet()) {
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
            }
            if (!recycleFinish) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10L));
            }
        }
        return recycleFinish;
    }
}
