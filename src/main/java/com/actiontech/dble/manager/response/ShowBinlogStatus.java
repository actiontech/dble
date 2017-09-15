/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkParamCfg;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.BinlogPause;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.SQLQueryResult;
import com.actiontech.dble.sqlengine.SQLQueryResultListener;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.config.loader.zkprocess.zookeeper.process.BinlogPause.BinlogPauseStatus;

public final class ShowBinlogStatus {
    private ShowBinlogStatus() {
    }

    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS_PACKET = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final String[] FIELDS = new String[]{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"};
    private static volatile boolean waiting = false;

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
    private static String errMsg = null;

    public static void execute(ManagerConnection c) {
        boolean isUseZK = DbleServer.getInstance().isUseZK();
        long timeout = DbleServer.getInstance().getConfig().getSystem().getShowBinlogStatusTimeout();
        if (isUseZK) {
            CuratorFramework zkConn = ZKUtils.getConnection();
            String lockPath = KVPathUtil.getBinlogPauseLockPath();
            InterProcessMutex distributeLock = new InterProcessMutex(zkConn, lockPath);
            try {
                //zkLock, the other instance cant't get lock before finished
                if (!distributeLock.acquire(100, TimeUnit.MILLISECONDS)) {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
                    return;
                }
                try {
                    if (!DbleServer.getInstance().getBackupLocked().compareAndSet(false, true)) {
                        c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
                    } else {
                        errMsg = null;
                        //notify zk to wait all session
                        String binlogStatusPath = KVPathUtil.getBinlogPauseStatus();
                        String binlogPause = KVPathUtil.getBinlogPauseInstance();
                        BinlogPause pauseOnInfo = new BinlogPause(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), BinlogPauseStatus.ON);
                        zkConn.setData().forPath(binlogStatusPath, pauseOnInfo.toString().getBytes(StandardCharsets.UTF_8));
                        long beginTime = TimeUtil.currentTimeMillis();
                        boolean isAllPaused = waitAllSession(c, timeout, beginTime);
                        if (isAllPaused) {
                            //tell zk this instance has prepared
                            ZKUtils.createTempNode(binlogPause, ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID));
                            //check all session waiting status
                            List<String> preparedList = zkConn.getChildren().forPath(binlogPause);
                            List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                            // TODO: While waiting, a new instance of MyCat is upping and working.
                            while (preparedList.size() < onlineList.size()) {
                                //TIMEOUT
                                if (TimeUtil.currentTimeMillis() > beginTime + timeout) {
                                    isAllPaused = false;
                                    errMsg = "timeout while waiting for unfinished distributed transactions.";
                                    logger.warn(errMsg);
                                    BinlogPause pauseTimeoutInfo = new BinlogPause(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), BinlogPauseStatus.TIMEOUT);
                                    zkConn.setData().forPath(binlogStatusPath, pauseTimeoutInfo.toString().getBytes(StandardCharsets.UTF_8));
                                    break;
                                }
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                                onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                                preparedList = zkConn.getChildren().forPath(binlogPause);
                            }
                            if (isAllPaused) {
                                getQueryResult(c.getCharset().getResults());
                            }
                            writeResponse(c);
                            BinlogPause pauseOffInfo = new BinlogPause(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), BinlogPauseStatus.OFF);
                            zkConn.setData().forPath(binlogStatusPath, pauseOffInfo.toString().getBytes(StandardCharsets.UTF_8));
                            zkConn.delete().forPath(ZKPaths.makePath(binlogPause, ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID)));
                            List<String> releaseList = zkConn.getChildren().forPath(binlogPause);
                            while (releaseList.size() != 0) {
                                releaseList = zkConn.getChildren().forPath(binlogPause);
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                            }
                        } else {
                            writeResponse(c);
                            BinlogPause pauseOffInfo = new BinlogPause(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID), BinlogPauseStatus.OFF);
                            zkConn.setData().forPath(binlogStatusPath, pauseOffInfo.toString().getBytes(StandardCharsets.UTF_8));
                        }
                    }
                } catch (Exception e) {
                    logger.warn("catch Exception", e);
                } finally {
                    DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
                    distributeLock.release();
                }
            } catch (Exception e) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
            }
        } else {
            if (!DbleServer.getInstance().getBackupLocked().compareAndSet(false, true)) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
            } else {
                try {
                    errMsg = null;
                    if (waitAllSession(c, timeout, TimeUtil.currentTimeMillis())) {
                        getQueryResult(c.getCharset().getResults());
                    }
                    writeResponse(c);
                } finally {
                    DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
                }
            }
        }
    }

    private static void writeResponse(ManagerConnection c) {
        if (errMsg == null) {
            ByteBuffer buffer = c.allocate();
            buffer = HEADER.write(buffer, c, true);
            for (FieldPacket field : FIELDS_PACKET) {
                buffer = field.write(buffer, c, true);
            }
            buffer = EOF.write(buffer, c, true);
            byte packetId = EOF.getPacketId();
            for (RowDataPacket row : rows) {
                row.setPacketId(++packetId);
                buffer = row.write(buffer, c, true);
            }
            rows.clear();
            EOFPacket lastEof = new EOFPacket();
            lastEof.setPacketId(++packetId);
            buffer = lastEof.write(buffer, c, true);
            c.write(buffer);
        } else {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, errMsg);
            errMsg = null;
        }
    }

    public static boolean isWaiting() {
        return waiting;
    }

    public static void setWaiting(boolean waiting) {
        ShowBinlogStatus.waiting = waiting;
    }

    public static boolean waitAllSession(String from) {
        logger.info("waiting all sessions of distributed transaction which are not finished.");
        long timeout = DbleServer.getInstance().getConfig().getSystem().getShowBinlogStatusTimeout();
        long beginTime = TimeUtil.currentTimeMillis();
        waiting = true;
        List<NonBlockingSession> fcList = getNeedWaitSession();
        while (!fcList.isEmpty()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            Iterator<NonBlockingSession> sListIterator = fcList.iterator();
            while (sListIterator.hasNext()) {
                NonBlockingSession session = sListIterator.next();
                if (!session.isNeedWaitFinished()) {
                    sListIterator.remove();
                }
            }
            if (!waiting) {
                waiting = false;
                logger.warn("stop waiting all sessions of distributed transaction");
                return false;
            }
            if ((TimeUtil.currentTimeMillis() > beginTime + timeout)) {
                try {
                    if (ZKUtils.getConnection().getChildren().forPath(KVPathUtil.getOnlinePath()).contains(from)) {
                        logger.warn("timeout and the from server node " + from + " is offline");
                        waiting = false;
                        DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
                        logger.warn("stop waiting all sessions of distributed transaction");
                        return false;
                    }
                } catch (Exception e) {
                    logger.warn("timeout and try to check server node " + from + " failed", e);
                    waiting = false;
                    DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
                    logger.warn("stop waiting all sessions of distributed transaction");
                    return false;
                }
            }
        }
        waiting = false;
        logger.info("all sessions of distributed transaction  are paused.");
        return true;
    }

    private static boolean waitAllSession(ManagerConnection c, long timeout, long beginTime) {
        logger.info("waiting all sessions of distributed transaction which are not finished.");
        List<NonBlockingSession> fcList = getNeedWaitSession();
        while (!fcList.isEmpty()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            Iterator<NonBlockingSession> sListIterator = fcList.iterator();
            while (sListIterator.hasNext()) {
                NonBlockingSession session = sListIterator.next();
                if (!session.isNeedWaitFinished()) {
                    sListIterator.remove();
                }
            }
            if (c.isClosed()) {
                errMsg = "client closed while waiting for unfinished distributed transactions.";
                logger.warn(errMsg);
                return false;
            }
            if (TimeUtil.currentTimeMillis() > beginTime + timeout) {
                errMsg = "timeout while waiting for unfinished distributed transactions.";
                logger.warn(errMsg);
                return false;
            }
        }
        logger.info("all sessions of distributed transaction  are paused.");
        return true;
    }

    private static List<NonBlockingSession> getNeedWaitSession() {
        List<NonBlockingSession> fcList = new ArrayList<>();
        for (NIOProcessor process : DbleServer.getInstance().getProcessors()) {
            for (FrontendConnection front : process.getFrontends().values()) {
                if (!(front instanceof ServerConnection)) {
                    continue;
                }
                ServerConnection sc = (ServerConnection) front;
                NonBlockingSession session = sc.getSession2();
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
        Collection<PhysicalDBPool> allPools = DbleServer.getInstance().getConfig().getDataHosts().values();
        sourceCount = new AtomicInteger(allPools.size());
        rows = new ArrayList<>(allPools.size());
        for (PhysicalDBPool pool : allPools) {
            //if WRITE_RANDOM_NODE ,may the binlog is not ready.
            final PhysicalDatasource source = pool.getSource();
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
            SQLJob sqlJob = new SQLJob(SHOW_BINLOG_QUERY, pool.getSchemas()[0], resultHandler, source);
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
