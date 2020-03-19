/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.cluster.*;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.cluster.kVtoXml.ClusterToXml;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
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
import com.actiontech.dble.singleton.ClusterGeneralConfig;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.cluster.ClusterPathUtil.SEPARATOR;
import static com.actiontech.dble.config.loader.zkprocess.zookeeper.process.BinlogPause.BinlogPauseStatus;

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
        boolean isUseZK = ClusterGeneralConfig.isUseZK();
        long timeout = DbleServer.getInstance().getConfig().getSystem().getShowBinlogStatusTimeout();
        if (isUseZK) {
            showBinlogWithZK(c, timeout);
        } else if (ClusterGeneralConfig.isUseGeneralCluster()) {
            showBinlogWithUcore(c, timeout);
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
        DistributeLock distributeLock = new DistributeLock(ClusterPathUtil.getBinlogPauseLockPath(), ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
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
                    //step 3 notify other dble to stop the commit & set self status
                    BinlogPause pauseOnInfo = new BinlogPause(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), BinlogPauseStatus.ON);

                    //step 4 wait til other dbles to feedback the ucore flag
                    long beginTime = TimeUtil.currentTimeMillis();
                    boolean isPaused = waitAllSession(c, timeout, beginTime);
                    if (!isPaused) {
                        writeResponse(c);
                        return;
                    }
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
                    BinlogPause pauseOffInfo = new BinlogPause(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), BinlogPauseStatus.OFF);
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
                    BinlogPause pauseOnInfo = new BinlogPause(ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), BinlogPauseStatus.ON);
                    zkConn.setData().forPath(binlogStatusPath, pauseOnInfo.toString().getBytes(StandardCharsets.UTF_8));
                    long beginTime = TimeUtil.currentTimeMillis();
                    boolean isPaused = waitAllSession(c, timeout, beginTime);

                    //tell zk this instance has prepared
                    ZKUtils.createTempNode(binlogPause, ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), String.valueOf(isPaused).getBytes(StandardCharsets.UTF_8));
                    //check all session waiting status
                    List<String> preparedList = zkConn.getChildren().forPath(binlogPause);
                    List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                    // TODO: While waiting, a new instance of dble is upping and working.

                    boolean isAllSuccess = true;
                    while (preparedList.size() < onlineList.size()) {
                        if (TimeUtil.currentTimeMillis() > beginTime + 2 * timeout) {
                            isAllSuccess = false;
                            errMsg = "timeout while waiting for unfinished distributed transactions.";
                            logger.info(errMsg);
                            break;
                        }
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                        onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                        preparedList = zkConn.getChildren().forPath(binlogPause);
                    }
                    if (isAllSuccess) {
                        for (String preparedNode : preparedList) {
                            String preparePath = ZKPaths.makePath(binlogPause, preparedNode);
                            byte[] resultStatus = zkConn.getData().forPath(preparePath);
                            String data = new String(resultStatus, StandardCharsets.UTF_8);
                            if (!Boolean.parseBoolean(data)) {
                                isAllSuccess = false;
                            }
                        }
                    }
                    if (isAllSuccess) {
                        getQueryResult(c.getCharset().getResults());
                    }
                    writeResponse(c);
                    BinlogPause pauseOffInfo = new BinlogPause(ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), BinlogPauseStatus.OFF);
                    zkConn.setData().forPath(binlogStatusPath, pauseOffInfo.toString().getBytes(StandardCharsets.UTF_8));
                    zkConn.delete().forPath(ZKPaths.makePath(binlogPause, ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID)));
                    List<String> releaseList = zkConn.getChildren().forPath(binlogPause);
                    while (releaseList.size() != 0) {
                        releaseList = zkConn.getChildren().forPath(binlogPause);
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                    }
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
        long timeout = DbleServer.getInstance().getConfig().getSystem().getShowBinlogStatusTimeout();
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
        Collection<PhysicalDataHost> allPools = DbleServer.getInstance().getConfig().getDataHosts().values();
        sourceCount = new AtomicInteger(allPools.size());
        rows = new CopyOnWriteArrayList<>();
        for (PhysicalDataHost pool : allPools) {
            //if WRITE_RANDOM_NODE ,may the binlog is not ready.
            final PhysicalDataSource source = pool.getWriteSource();
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
