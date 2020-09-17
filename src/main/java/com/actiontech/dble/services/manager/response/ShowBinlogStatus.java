/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

public final class ShowBinlogStatus {
    private ShowBinlogStatus() {
    }

    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS_PACKET = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final String[] FIELDS = new String[]{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"};

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.setPacketId(++packetId);
        FIELDS_PACKET[i] = PacketUtil.getField("Url", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS_PACKET[i++].setPacketId(++packetId);
        for (String field : FIELDS) {
            FIELDS_PACKET[i] = PacketUtil.getField(field, Fields.FIELD_TYPE_VAR_STRING);
            FIELDS_PACKET[i++].setPacketId(++packetId);
        }
        EOF.setPacketId(++packetId);
    }

    private static final String SHOW_BINLOG_QUERY = "SHOW MASTER STATUS";
    private static Logger logger = LoggerFactory.getLogger(ShowBinlogStatus.class);
    private static AtomicInteger sourceCount;
    private static List<RowDataPacket> rows;
    private static volatile String errMsg = null;

    public static void execute(ManagerService service) {
        long timeout = ClusterConfig.getInstance().getShowBinlogStatusTimeout();
        if (ClusterConfig.getInstance().isClusterEnable()) {
            showBinlogWithCluster(service, timeout);
        } else {
            if (!DbleServer.getInstance().getBackupLocked().compareAndSet(false, true)) {
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
            } else {
                try {
                    errMsg = null;
                    if (waitAllSession(service, timeout, TimeUtil.currentTimeMillis())) {
                        getQueryResult(service.getCharset().getResults());
                    }
                    writeResponse(service);
                } finally {
                    DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
                }
            }
        }
    }


    private static void showBinlogWithCluster(ManagerService service, long timeout) {
        //step 1 get the distributeLock
        DistributeLock distributeLock = ClusterHelper.createDistributeLock(ClusterPathUtil.getBinlogPauseLockPath(), SystemConfig.getInstance().getInstanceName());
        try {
            if (!distributeLock.acquire()) {
                service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
                return;
            }
            try {
                //step 2 try to lock all the commit flag in server
                if (!DbleServer.getInstance().getBackupLocked().compareAndSet(false, true)) {
                    service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
                } else {

                    //step 3 wait til other dbles to feedback the ucore flag
                    errMsg = null;
                    long beginTime = TimeUtil.currentTimeMillis();
                    boolean isPaused = waitAllSession(service, timeout, beginTime);
                    if (!isPaused) {
                        writeResponse(service);
                        return;
                    }
                    //step 4 notify other dble to stop the commit & set self status
                    String binlogStatusPath = ClusterPathUtil.getBinlogPauseStatus();
                    ClusterHelper.setKV(binlogStatusPath, SystemConfig.getInstance().getInstanceName());
                    ClusterHelper.createSelfTempNode(binlogStatusPath, ClusterPathUtil.SUCCESS);

                    Map<String, String> expectedMap = ClusterHelper.getOnlineMap();
                    while (true) {
                        StringBuffer errorStringBuf = new StringBuffer();
                        if (ClusterLogic.checkResponseForOneTime(ClusterPathUtil.SUCCESS, binlogStatusPath, expectedMap, errorStringBuf)) {
                            errMsg = errorStringBuf.length() <= 0 ? null : errorStringBuf.toString();
                            break;
                        } else if (TimeUtil.currentTimeMillis() > beginTime + 2 * timeout) {
                            errMsg = "timeout while waiting for unfinished distributed transactions.";
                            logger.info(errMsg);
                            break;
                        }
                    }

                    // step 6 query for the GTID and write back to frontend connections
                    if (errMsg == null) {
                        getQueryResult(service.getCharset().getResults());
                    }
                    writeResponse(service);

                    //step 7 delete the KVtree and notify the cluster
                    ClusterHelper.cleanPath(binlogStatusPath);

                }
            } catch (Exception e) {
                logger.warn("catch Exception", e);
            } finally {
                DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
                distributeLock.release();
            }
        } catch (Exception e) {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
        }

    }


    private static void writeResponse(ManagerService service) {
        if (errMsg == null) {
            ByteBuffer buffer = service.allocate();
            buffer = HEADER.write(buffer, service, true);
            for (FieldPacket field : FIELDS_PACKET) {
                buffer = field.write(buffer, service, true);
            }
            buffer = EOF.write(buffer, service, true);
            byte packetId = EOF.getPacketId();
            for (RowDataPacket row : rows) {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, service, true);
            }
            rows.clear();
            EOFRowPacket lastEof = new EOFRowPacket();
            lastEof.setPacketId(++packetId);

            lastEof.write(buffer, service);
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, errMsg);
            errMsg = null;
        }
    }

    public static boolean waitAllSession() {
        logger.info("waiting all sessions of distributed transaction which are not finished.");
        long timeout = ClusterConfig.getInstance().getShowBinlogStatusTimeout();
        long beginTime = TimeUtil.currentTimeMillis();
        List<NonBlockingSession> fcList = getNeedWaitSession();
        while (!fcList.isEmpty()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            fcList.removeIf(session -> !session.isNeedWaitFinished());
            if ((TimeUtil.currentTimeMillis() > beginTime + timeout)) {
                logger.info("wait session finished timeout");
                return false;
            }
        }
        logger.info("all sessions of distributed transaction  are paused.");
        return true;
    }

    private static boolean waitAllSession(ManagerService service, long timeout, long beginTime) {
        logger.info("waiting all sessions of distributed transaction which are not finished.");
        List<NonBlockingSession> fcList = getNeedWaitSession();
        while (!fcList.isEmpty()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            fcList.removeIf(session -> !session.isNeedWaitFinished());
            if (service.getConnection().isClosed()) {
                errMsg = "client closed while waiting for unfinished distributed transactions.";
                logger.info(errMsg);
                return false;
            }
            if (TimeUtil.currentTimeMillis() > beginTime + timeout) {
                errMsg = "timeout while waiting for unfinished distributed transactions.";
                logger.info(errMsg);
                return false;
            }
        }
        logger.info("all sessions of distributed transaction  are paused.");
        return true;
    }

    private static List<NonBlockingSession> getNeedWaitSession() {
        List<NonBlockingSession> fcList = new ArrayList<>();
        for (IOProcessor process : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection front : process.getFrontends().values()) {
                if (front.isManager()) {
                    continue;
                }
                NonBlockingSession session = ((ShardingService) front.getService()).getSession2();
                if (session.isNeedWaitFinished()) {
                    fcList.add(session);
                }
            }
        }
        return fcList;
    }

    /**
     * getQueryResult: show master status
     *
     * @param charset
     */
    private static void getQueryResult(final String charset) {
        Map<String, ShardingNode> shardingNodes = DbleServer.getInstance().getConfig().getShardingNodes();
        Set<PhysicalDbGroup> dbGroupSet = shardingNodes.values().stream().map(ShardingNode::getDbGroup).collect(Collectors.toSet());
        sourceCount = new AtomicInteger(dbGroupSet.size());
        rows = new CopyOnWriteArrayList<>();
        for (PhysicalDbGroup pool : dbGroupSet) {
            //if WRITE_RANDOM_NODE ,may the binlog is not ready.
            final PhysicalDbInstance source = pool.getWriteDbInstance();
            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(FIELDS,
                    new SQLQueryResultListener<SQLQueryResult<Map<String, String>>>() {
                        @Override
                        public void onResult(SQLQueryResult<Map<String, String>> result) {
                            String url = source.getConfig().getUrl();
                            if (!result.isSuccess()) {
                                errMsg = "Getting binlog status from this instance[" + url + "] is failed";
                            } else {
                                rows.add(getRow(url, result.getResult(), charset));
                            }
                            sourceCount.decrementAndGet();
                        }

                    });
            SQLJob sqlJob = new SQLJob(SHOW_BINLOG_QUERY, null, resultHandler, source);
            sqlJob.run();
        }
        while (sourceCount.get() > 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    private static RowDataPacket getRow(String url, Map<String, String> result, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(url, charset));
        for (String field : FIELDS) {
            row.add(StringUtil.encode(result.get(field), charset));
        }
        return row;
    }
}
