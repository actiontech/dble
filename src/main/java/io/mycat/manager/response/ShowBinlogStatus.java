package io.mycat.manager.response;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.zookeeper.process.BinlogPause;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.FrontendConnection;
import io.mycat.net.NIOProcessor;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;
import io.mycat.util.KVPathUtil;
import io.mycat.util.StringUtil;
import io.mycat.util.TimeUtil;
import io.mycat.util.ZKUtils;
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

import static io.mycat.config.loader.zkprocess.zookeeper.process.BinlogPause.BinlogPauseStatus;

public class ShowBinlogStatus {
    private static final int FIELD_COUNT = 6;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    private static final String[] FIELDS = new String[]{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"};
    private static volatile boolean waiting = false;

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("Url", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        for (String field : FIELDS) {
            fields[i] = PacketUtil.getField(field, Fields.FIELD_TYPE_VAR_STRING);
            fields[i++].packetId = ++packetId;
        }
        eof.packetId = ++packetId;
    }

    private static final String SHOW_BINLOG_QUERY = "SHOW MASTER STATUS";
    private static Logger logger = LoggerFactory.getLogger(ShowBinlogStatus.class);
    private static AtomicInteger sourceCount;
    private static List<RowDataPacket> rows;
    private static String errMsg = null;

    public static void execute(ManagerConnection c) {
        boolean isUseZK = MycatServer.getInstance().isUseZK();
        long timeout = MycatServer.getInstance().getConfig().getSystem().getShowBinlogStatusTimeout();
        if (isUseZK) {
            CuratorFramework zkConn = ZKUtils.getConnection();
            String lockPath = KVPathUtil.getBinlogPauseLockPath();
            InterProcessMutex distributeLock = new InterProcessMutex(zkConn, lockPath);
            try {
                //zkLock， the other instance cant't get lock before finished
                if (!distributeLock.acquire(100, TimeUnit.MILLISECONDS)) {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
                    return;
                }
                try {
                    if (!MycatServer.getInstance().getBackupLocked().compareAndSet(false, true)) {
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
                                getQueryResult(c.getCharset());
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
                    MycatServer.getInstance().getBackupLocked().compareAndSet(true, false);
                    distributeLock.release();
                }
            } catch (Exception e) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.getMessage());
            }
        } else {
            if (!MycatServer.getInstance().getBackupLocked().compareAndSet(false, true)) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "There is another command is showing BinlogStatus");
            } else {
                try {
                    errMsg = null;
                    if (waitAllSession(c, timeout, TimeUtil.currentTimeMillis())) {
                        getQueryResult(c.getCharset());
                    }
                    writeResponse(c);
                } finally {
                    MycatServer.getInstance().getBackupLocked().compareAndSet(true, false);
                }
            }
        }
    }

    private static void writeResponse(ManagerConnection c) {
        if (errMsg == null) {
            ByteBuffer buffer = c.allocate();
            buffer = header.write(buffer, c, true);
            for (FieldPacket field : fields) {
                buffer = field.write(buffer, c, true);
            }
            buffer = eof.write(buffer, c, true);
            byte packetId = eof.packetId;
            for (RowDataPacket row : rows) {
                row.packetId = ++packetId;
                buffer = row.write(buffer, c, true);
            }
            rows.clear();
            EOFPacket lastEof = new EOFPacket();
            lastEof.packetId = ++packetId;
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
        long timeout = MycatServer.getInstance().getConfig().getSystem().getShowBinlogStatusTimeout();
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
                        MycatServer.getInstance().getBackupLocked().compareAndSet(true, false);
                        logger.warn("stop waiting all sessions of distributed transaction");
                        return false;
                    }
                } catch (Exception e) {
                    logger.warn("timeout and try to check server node " + from + " failed", e);
                    waiting = false;
                    MycatServer.getInstance().getBackupLocked().compareAndSet(true, false);
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
        for (NIOProcessor process : MycatServer.getInstance().getProcessors()) {
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
        Collection<PhysicalDBPool> allPools = MycatServer.getInstance().getConfig().getDataHosts().values();
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
