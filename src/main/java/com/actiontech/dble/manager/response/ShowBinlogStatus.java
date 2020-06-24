/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.general.ClusterGeneralDistributeLock;
import com.actiontech.dble.cluster.general.kVtoXml.ClusterToXml;
import com.actiontech.dble.cluster.zkprocess.ZkDistributeLock;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.BinlogPause;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
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
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.cluster.ClusterPathUtil.SEPARATOR;
import static com.actiontech.dble.cluster.zkprocess.zookeeper.process.BinlogPause.BinlogPauseStatus;

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

    public static void execute(ManagerConnection c) {
        long timeout = ClusterConfig.getInstance().getShowBinlogStatusTimeout();
        if (ClusterConfig.getInstance().isClusterEnable()) {
            if (ClusterConfig.getInstance().useZkMode()) {
                showBinlogWithZK(c, timeout);
            } else {
                showBinlogWithUcore(c, timeout);
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


    private static void showBinlogWithUcore(ManagerConnection c, long timeout) {

        //step 1 get the distributeLock of the ucore
        ClusterGeneralDistributeLock distributeLock = new ClusterGeneralDistributeLock(ClusterPathUtil.getBinlogPauseLockPath(), SystemConfig.getInstance().getInstanceName());
        try {
            if (!distributeLock.acquire()) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
                return;
            }
            try {
                //step 2 try to lock all the commit flag in server
                if (!DbleServer.getInstance().getBackupLocked().compareAndSet(false, true)) {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
                } else {

                    //step 3 wait til other dbles to feedback the ucore flag
                    long beginTime = TimeUtil.currentTimeMillis();
                    boolean isPaused = waitAllSession(c, timeout, beginTime);
                    if (!isPaused) {
                        writeResponse(c);
                        return;
                    }
                    //step 4 notify other dble to stop the commit & set self status
                    BinlogPause pauseOnInfo = new BinlogPause(SystemConfig.getInstance().getInstanceName(), BinlogPauseStatus.ON);

                    ClusterHelper.setKV(ClusterPathUtil.getBinlogPauseStatus(), pauseOnInfo.toString());
                    ClusterHelper.setKV(ClusterPathUtil.getBinlogPauseStatusSelf(), ClusterPathUtil.SUCCESS);

                    Map<String, String> expectedMap = ClusterToXml.getOnlineMap();
                    while (true) {
                        StringBuffer errorStringBuf = new StringBuffer();
                        if (ClusterHelper.checkResponseForOneTime(ClusterPathUtil.SUCCESS, ClusterPathUtil.getBinlogPauseStatus(), expectedMap, errorStringBuf)) {
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
                        getQueryResult(c.getCharset().getResults());
                    }
                    writeResponse(c);

                    //step 7 delete the KVtree and notify the cluster
                    ClusterHelper.cleanPath(ClusterPathUtil.getBinlogPauseStatus() + SEPARATOR);
                    BinlogPause pauseOffInfo = new BinlogPause(SystemConfig.getInstance().getInstanceName(), BinlogPauseStatus.OFF);
                    ClusterHelper.setKV(ClusterPathUtil.getBinlogPauseStatus(), pauseOffInfo.toString());

                }
            } catch (Exception e) {
                logger.info("catch Exception", e);
            } finally {
                DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
                distributeLock.release();
            }
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
        }

    }

    private static void showBinlogWithZK(ManagerConnection c, long timeout) {
        CuratorFramework zkConn = ZKUtils.getConnection();
        String lockPath = ClusterPathUtil.getBinlogPauseLockPath();
        DistributeLock distributeLock = new ZkDistributeLock(lockPath, String.valueOf(System.currentTimeMillis()));
        //zkLock, the other instance cant't get lock before finished
        if (!distributeLock.acquire()) {
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
            return;
        }
        try {
            if (!DbleServer.getInstance().getBackupLocked().compareAndSet(false, true)) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
            } else {
                errMsg = null;
                long beginTime = TimeUtil.currentTimeMillis();
                boolean isPaused = waitAllSession(c, timeout, beginTime);
                if (!isPaused) {
                    writeResponse(c);
                    return;
                }
                //notify zk to wait all session
                String binlogStatusPath = ClusterPathUtil.getBinlogPauseStatus();
                BinlogPause pauseOnInfo = new BinlogPause(SystemConfig.getInstance().getInstanceName(), BinlogPauseStatus.ON);
                zkConn.setData().forPath(binlogStatusPath, pauseOnInfo.toString().getBytes(StandardCharsets.UTF_8));

                //tell zk this instance has prepared
                ZKUtils.createTempNode(binlogStatusPath, SystemConfig.getInstance().getInstanceName(), ClusterPathUtil.SUCCESS.getBytes(StandardCharsets.UTF_8));
                //check all session waiting status
                List<String> preparedList = zkConn.getChildren().forPath(binlogStatusPath);
                List<String> onlineList = zkConn.getChildren().forPath(ClusterPathUtil.getOnlinePath());
                // TODO: While waiting, a new instance of dble is upping and working.

                while (preparedList.size() < onlineList.size()) {
                    if (TimeUtil.currentTimeMillis() > beginTime + 2 * timeout) {
                        errMsg = "timeout while waiting for unfinished distributed transactions.";
                        logger.info(errMsg);
                        break;
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                    onlineList = zkConn.getChildren().forPath(ClusterPathUtil.getOnlinePath());
                    preparedList = zkConn.getChildren().forPath(binlogStatusPath);
                }
                if (errMsg == null) {
                    for (String preparedNode : preparedList) {
                        String preparePath = ZKPaths.makePath(binlogStatusPath, preparedNode);
                        byte[] resultStatus = zkConn.getData().forPath(preparePath);
                        String data = new String(resultStatus, StandardCharsets.UTF_8);
                        if (!ClusterPathUtil.SUCCESS.equals(data)) {
                            errMsg = "timeout while waiting for unfinished distributed transactions.";
                        }
                    }
                }
                if (errMsg == null) {
                    getQueryResult(c.getCharset().getResults());
                }
                writeResponse(c);
                BinlogPause pauseOffInfo = new BinlogPause(SystemConfig.getInstance().getInstanceName(), BinlogPauseStatus.OFF);
                zkConn.setData().forPath(binlogStatusPath, pauseOffInfo.toString().getBytes(StandardCharsets.UTF_8));
                zkConn.delete().forPath(ZKPaths.makePath(binlogStatusPath, SystemConfig.getInstance().getInstanceName()));
                List<String> releaseList = zkConn.getChildren().forPath(binlogStatusPath);
                while (releaseList.size() != 0) {
                    releaseList = zkConn.getChildren().forPath(binlogStatusPath);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                }
            }
        } catch (Exception e) {
            logger.info("catch Exception", e);
        } finally {
            DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
            distributeLock.release();
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

    public static boolean waitAllSession() {
        logger.info("waiting all sessions of distributed transaction which are not finished.");
        long timeout = ClusterConfig.getInstance().getShowBinlogStatusTimeout();
        long beginTime = TimeUtil.currentTimeMillis();
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
            if ((TimeUtil.currentTimeMillis() > beginTime + timeout)) {
                logger.info("wait session finished timeout");
                return false;
            }
        }
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
        for (NIOProcessor process : DbleServer.getInstance().getFrontProcessors()) {
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
        Collection<PhysicalDbGroup> allPools = DbleServer.getInstance().getConfig().getDbGroups().values();
        sourceCount = new AtomicInteger(allPools.size());
        rows = new CopyOnWriteArrayList<>();
        for (PhysicalDbGroup pool : allPools) {
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
