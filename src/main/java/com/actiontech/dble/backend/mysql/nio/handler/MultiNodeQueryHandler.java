/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.mysql.LoadDataUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AutoCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AutoTxOperation;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionHandler;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.FlowCotrollerConfig;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.singleton.CacheService;
import com.actiontech.dble.singleton.WriteQueueFlowController;
import com.actiontech.dble.statistic.stat.QueryResult;
import com.actiontech.dble.statistic.stat.QueryResultDispatcher;
import com.actiontech.dble.util.DebugPauseUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * @author mycat
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements LoadDataResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeQueryHandler.class);
    protected final RouteResultset rrs;
    protected final boolean sessionAutocommit;
    private long affectedRows;
    long selectRows;
    protected List<BackendConnection> errConnection;
    private long netOutBytes;
    private long resultSize;
    protected boolean prepared;
    protected ErrorPacket err;
    protected int fieldCount = 0;
    volatile boolean fieldsReturned;
    private long insertId;
    private String cacheKeyTable = null;
    private int cacheKeyIndex = -1;
    private List<FieldPacket> fieldPackets = new ArrayList<>();
    protected volatile ByteBuffer byteBuffer;
    protected Set<BackendConnection> closedConnSet;
    private final boolean modifiedSQL;
    protected Set<RouteResultsetNode> connRrns = new ConcurrentSkipListSet<>();
    private Map<String, Integer> dataNodePauseInfo; // only for debug
    private AtomicBoolean recycledBuffer = new AtomicBoolean(false);

    public MultiNodeQueryHandler(RouteResultset rrs, NonBlockingSession session) {
        this(rrs, session, true);
    }

    protected MultiNodeQueryHandler(RouteResultset rrs, NonBlockingSession session, boolean createBufferIfNeed) {
        super(session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute multi node query " + rrs.getStatement());
        }
        this.rrs = rrs;
        if (createBufferIfNeed) {
            for (RouteResultsetNode node : rrs.getNodes()) {
                if (ServerParse.SELECT == node.getSqlType()) {
                    byteBuffer = session.getSource().allocate();
                    break;
                }
            }
        }
        this.sessionAutocommit = session.getSource().isAutocommit();
        this.modifiedSQL = rrs.getNodes()[0].isModifySQL();
        initDebugInfo();
    }

    @Override
    protected void reset() {
        super.reset();
        if (rrs.isLoadData()) {
            packetId = session.getSource().getLoadDataInfileHandler().getLastPackId();
        }
        connRrns.clear();
        this.netOutBytes = 0;
        this.resultSize = 0;
    }

    public NonBlockingSession getSession() {
        return session;
    }

    public void execute() throws Exception {
        lock.lock();
        try {
            this.reset();
            this.fieldsReturned = false;
            this.affectedRows = 0L;
            this.insertId = 0L;
        } finally {
            lock.unlock();
        }
        LOGGER.debug("rrs.getRunOnSlave()-" + rrs.getRunOnSlave());
        StringBuilder sb = new StringBuilder();
        for (final RouteResultsetNode node : rrs.getNodes()) {
            unResponseRrns.add(node);
            if (node.isModifySQL()) {
                sb.append("[").append(node.getName()).append("]").append(node.getStatement()).append(";\n");
            }
        }
        if (sb.length() > 0) {
            TxnLogHelper.putTxnLog(session.getSource(), sb.toString());
        }
        for (final RouteResultsetNode node : rrs.getNodes()) {
            BackendConnection conn = session.getTarget(node);
            if (session.tryExistsCon(conn, node)) {
                node.setRunOnSlave(rrs.getRunOnSlave());
                innerExecute(conn, node);
            } else {
                connRrns.add(node);
                // create new connection
                node.setRunOnSlave(rrs.getRunOnSlave());
                PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), session.getSource().isTxStart(), sessionAutocommit, node, this, node);
            }
        }
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session)) {
            cleanBuffer();
            return;
        }
        MySQLConnection mysqlCon = (MySQLConnection) conn;
        mysqlCon.setResponseHandler(this);
        mysqlCon.setSession(session);
        mysqlCon.setComplexQuery(complexQuery);
        mysqlCon.executeMultiNode(node, session.getSource(), sessionAutocommit && !session.getSource().isTxStart() && !node.isModifySQL());
    }

    public void cleanBuffer() {
        if (recycledBuffer.compareAndSet(false, true)) {
            if (byteBuffer != null) {
                session.getSource().recycle(byteBuffer);
            }
        }
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        pauseTime(conn);
        if (checkClosedConn(conn)) {
            return;
        }
        LOGGER.warn("backend connect " + reason + ", conn info:" + conn);
        ErrorPacket errPacket = new ErrorPacket();
        byte lastPacketId = packetId;
        errPacket.setPacketId(++lastPacketId);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        reason = "Connection {DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "],threadID[" +
                ((MySQLConnection) conn).getThreadId() + "]} was closed ,reason is [" + reason + "]";
        errPacket.setMessage(StringUtil.encode(reason, session.getSource().getCharset().getResults()));
        err = errPacket;
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
            unResponseRrns.remove(rNode);
            session.getTargetMap().remove(rNode);
            conn.setResponseHandler(null);
            executeError(conn);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("Backend connect Error, Connection info:" + conn, e);
        ErrorPacket errPacket = new ErrorPacket();
        byte lastPacketId = packetId;
        errPacket.setPacketId(++lastPacketId);
        errPacket.setErrNo(ErrorCode.ER_DATA_HOST_ABORTING_CONNECTION);
        String errMsg = "Backend connect Error, Connection{DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "]} refused";
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        err = errPacket;
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            errorConnsCnt++;
            executeError(conn);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
        session.bindConnection(node, conn);
        connRrns.remove(node);
        innerExecute(conn, node);
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        pauseTime(conn);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(data);
        byte lastPacketId = packetId;
        errPacket.setPacketId(++lastPacketId); //just for normal error
        err = errPacket;
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            if (!isFail()) {
                setFail(new String(err.getMessage()));
            }
            if (errConnection == null) {
                errConnection = new ArrayList<>();
            }
            errConnection.add(conn);
            if (decrementToZero(conn)) {
                packetId++;
                if (session.closed()) {
                    cleanBuffer();
                } else if (byteBuffer != null) {
                    session.getSource().write(byteBuffer);
                }
                handleEndPacket(errPacket, AutoTxOperation.ROLLBACK, false);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        this.netOutBytes += data.length;
        boolean executeResponse = conn.syncAndExecute();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:" + executeResponse + " from " + conn);
        }
        if (executeResponse) {
            pauseTime(conn);
            this.resultSize += data.length;
            session.setBackendResponseEndTime((MySQLConnection) conn);
            ServerConnection source = session.getSource();
            OkPacket ok = new OkPacket();
            ok.read(data);
            lock.lock();
            try {
                // the affected rows of global table will use the last node's response
                if (!rrs.isGlobalTable()) {
                    affectedRows += ok.getAffectedRows();
                } else {
                    affectedRows = ok.getAffectedRows();
                }
                if (ok.getInsertId() > 0) {
                    insertId = (insertId == 0) ? ok.getInsertId() : Math.min(insertId, ok.getInsertId());
                }
                if (!decrementToZero(conn))
                    return;
                if (isFail()) {
                    session.resetMultiStatementStatus();
                    handleEndPacket(err, AutoTxOperation.ROLLBACK, false);
                    return;
                }
                ok.setPacketId(++packetId); // OK_PACKET
                if (rrs.isLoadData()) {
                    ok.setMessage(("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: 0").getBytes());
                    source.getLoadDataInfileHandler().clear();
                } else {
                    ok.setMessage(null);
                }

                ok.setAffectedRows(affectedRows);
                session.setRowCount(affectedRows);
                ok.setServerStatus(source.isAutocommit() ? 2 : 1);
                if (insertId > 0) {
                    ok.setInsertId(insertId);
                    source.setLastInsertId(insertId);
                }
                doSqlStat();
                handleEndPacket(ok, AutoTxOperation.COMMIT, true);
            } finally {
                lock.unlock();
            }
        }
    }


    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        this.netOutBytes += header.length;
        for (byte[] field : fields) {
            this.netOutBytes += field.length;
        }
        this.netOutBytes += eof.length;
        if (fieldsReturned) {
            return;
        }
        lock.lock();
        try {
            if (session.closed()) {
                cleanBuffer();
                return;
            }
            if (fieldsReturned) {
                return;
            }
            this.resultSize += header.length;
            for (byte[] field : fields) {
                this.resultSize += field.length;
            }
            this.resultSize += eof.length;
            fieldsReturned = true;
            executeFieldEof(header, fields, eof);
        } catch (Exception e) {
            cleanBuffer();
            handleDataProcessException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rowEofResponse(final byte[] eof, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("on row end response " + conn);
        }

        this.netOutBytes += eof.length;
        if (errorResponse.get()) {
            return;
        }
        RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
        final ServerConnection source = session.getSource();
        if (!rrs.isCallStatement()) {
            if (clearIfSessionClosed(session)) {
                cleanBuffer();
                return;
            } else {
                session.releaseConnectionIfSafe(conn, false);
            }
        }
        boolean zeroReached;
        lock.lock();
        try {
            unResponseRrns.remove(rNode);
            zeroReached = canResponse();
            if (zeroReached) {
                this.resultSize += eof.length;
                if (!rrs.isCallStatement()) {
                    if (this.sessionAutocommit && !session.getSource().isTxStart() && !session.getSource().isLocked()) { // clear all connections
                        session.releaseConnections(false);
                    }

                    if (this.isFail()) {
                        session.setResponseTime(false);
                        session.resetMultiStatementStatus();
                        if (session.closed()) {
                            cleanBuffer();
                        } else {
                            session.getSource().write(byteBuffer);
                        }
                        ErrorPacket errorPacket = createErrPkg(this.error);
                        handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
                        return;
                    }
                }

                if (session.closed()) {
                    cleanBuffer();
                } else {
                    boolean multiStatementFlag = session.multiStatementPacket(eof, ++packetId);
                    writeEofResult(eof, source);
                    session.multiStatementNextSql(multiStatementFlag);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketNull, boolean isLeft, BackendConnection conn) {
        this.netOutBytes += row.length;
        if (errorResponse.get()) {
            // the connection has been closed or set to "txInterrupt" properly
            //in tryErrorFinished() method! If we close it here, it can
            // lead to tx error such as blocking rollback tx for ever.
            // @author Uncle-pan
            // @since 2016-03-25
            //conn.close(error);
            return true;
        }
        lock.lock();
        try {
            if (session.closed()) {
                cleanBuffer();
            }
            this.selectRows++;
            RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
            String dataNode = rNode.getName();

            if (rrs.getLimitSize() >= 0) {
                if (selectRows <= rrs.getLimitStart() ||
                        (selectRows > (rrs.getLimitStart() < 0 ? 0 : rrs.getLimitStart()) + rrs.getLimitSize())) {
                    return false;
                }
            }
            this.resultSize += row.length;
            RowDataPacket rowDataPkg = null;
            // cache cacheKey-> dataNode
            boolean isBigPackage = row.length >= MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE;
            if (cacheKeyIndex != -1 && !isBigPackage) {
                rowDataPkg = new RowDataPacket(fieldCount);
                row[3] = packetId;
                rowDataPkg.read(row);
                byte[] key = rowDataPkg.fieldValues.get(cacheKeyIndex);
                if (key != null) {
                    String cacheKey = new String(rowDataPkg.fieldValues.get(cacheKeyIndex));
                    LayerCachePool pool = CacheService.getTableId2DataNodeCache();
                    if (pool != null) {
                        pool.putIfAbsent(cacheKeyTable, cacheKey, dataNode);
                    }
                }
            }
            if (!errorResponse.get()) {
                FlowCotrollerConfig fconfig = WriteQueueFlowController.getFlowCotrollerConfig();
                if (fconfig.isEnableFlowControl() &&
                        session.getSource().getWriteQueue().size() > fconfig.getStart()) {
                    session.getSource().startFlowControl(conn);
                }
                if (prepared) {
                    if (rowDataPkg == null) {
                        rowDataPkg = new RowDataPacket(fieldCount);
                        row[3] = ++packetId;
                        rowDataPkg.read(row);
                    }
                    BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
                    binRowDataPk.read(fieldPackets, rowDataPkg);
                    binRowDataPk.setPacketId(rowDataPkg.getPacketId());
                    byteBuffer = binRowDataPk.write(byteBuffer, session.getSource(), true);
                    this.packetId = (byte) session.getPacketId().get();
                } else {
                    if (isBigPackage) {
                        byteBuffer = session.getSource().writeBigPackageToBuffer(row, byteBuffer, packetId);
                        this.packetId = (byte) session.getPacketId().get();
                    } else {
                        row[3] = ++packetId;
                        byteBuffer = session.getSource().writeToBuffer(row, byteBuffer);
                    }
                }
            }
        } catch (Exception e) {
            cleanBuffer();
            handleDataProcessException(e);
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public void clearResources() {
        if (closedConnSet != null) {
            closedConnSet.clear();
        }
    }

    @Override
    public void writeQueueAvailable() {
    }

    @Override
    public void requestDataResponse(byte[] data, BackendConnection conn) {
        LoadDataUtil.requestFileDataResponse(data, conn);
    }


    private void executeError(BackendConnection conn) {
        if (!isFail()) {
            setFail(new String(err.getMessage()));
        }
        if (errConnection == null) {
            errConnection = new ArrayList<>();
        }
        errConnection.add(conn);
        if (conn.isClosed() && (!session.getSource().isAutocommit() || session.getSource().isTxStart())) {
            session.getSource().setTxInterrupt(error);
        }

        if (canResponse()) {
            packetId++;
            if (byteBuffer == null) {
                handleEndPacket(err, AutoTxOperation.ROLLBACK, false);
            } else if (session.closed()) {
                cleanBuffer();
            } else {
                session.getSource().write(byteBuffer);
                handleEndPacket(err, AutoTxOperation.ROLLBACK, false);
            }
        }
    }

    private void writeEofResult(byte[] eof, ServerConnection source) {
        eof[3] = packetId;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("last packet id:" + packetId);
        }
        byteBuffer = source.writeToBuffer(eof, byteBuffer);
        session.setResponseTime(true);
        doSqlStat();
        source.write(byteBuffer);
    }

    void doSqlStat() {
        if (DbleServer.getInstance().getConfig().getSystem().getUseSqlStat() == 1) {
            long netInBytes = 0;
            if (rrs != null && rrs.getStatement() != null) {
                netInBytes += rrs.getStatement().getBytes().length;
            }
            assert rrs != null;
            QueryResult queryResult = new QueryResult(session.getSource().getUser(), rrs.getSqlType(),
                    rrs.getStatement(), selectRows, netInBytes, netOutBytes, session.getQueryStartTime(), System.currentTimeMillis(), resultSize);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try to record sql:" + rrs.getStatement());
            }
            QueryResultDispatcher.dispatchQuery(queryResult);
        }
    }

    private void executeFieldEof(byte[] header, List<byte[]> fields, byte[] eof) {
        ServerConnection source = session.getSource();
        fieldCount = fields.size();
        header[3] = ++packetId;
        byteBuffer = source.writeToBuffer(header, byteBuffer);
        String cacheKey = null;
        if (rrs.hasCacheKeyToCache()) {
            String[] items = rrs.getCacheKeyItems();
            cacheKeyTable = items[0];
            cacheKey = items[1];
        }

        if (!errorResponse.get()) {
            for (int i = 0, len = fieldCount; i < len; ++i) {
                byte[] field = fields.get(i);
                FieldPacket fieldPkg = new FieldPacket();
                fieldPkg.read(field);
                if (rrs.getSchema() != null) {
                    fieldPkg.setDb(rrs.getSchema().getBytes());
                }
                if (rrs.getTableAlias() != null) {
                    fieldPkg.setTable(rrs.getTableAlias().getBytes());
                }
                if (rrs.getTable() != null) {
                    fieldPkg.setOrgTable(rrs.getTable().getBytes());
                }
                fieldPackets.add(fieldPkg);
                fieldCount = fields.size();
                if (cacheKey != null && cacheKeyIndex == -1) {
                    // find primary key index
                    String fieldName = new String(fieldPkg.getName());
                    if (cacheKey.equalsIgnoreCase(fieldName)) {
                        cacheKeyIndex = i;
                    }
                }
                fieldPkg.setPacketId(++packetId);
                byteBuffer = fieldPkg.write(byteBuffer, source, false);
            }
            eof[3] = ++packetId;
            byteBuffer = source.writeToBuffer(eof, byteBuffer);
        }
    }


    void handleDataProcessException(Exception e) {
        if (!errorResponse.get()) {
            this.error = e.toString();
            LOGGER.info("caught exception ", e);
            setFail(e.toString());
            this.tryErrorFinished(true);
        }
    }

    private boolean checkClosedConn(BackendConnection conn) {
        lock.lock();
        try {
            if (closedConnSet == null) {
                closedConnSet = new HashSet<>(1);
                closedConnSet.add(conn);
            } else {
                if (closedConnSet.contains(conn)) {
                    return true;
                }
                closedConnSet.add(conn);
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    void handleEndPacket(OkPacket packet, AutoTxOperation txOperation, boolean isSuccess) {
        session.multiStatementPacket(packet, packet.getPacketId());
        handleEndPacketInner(packet, txOperation, isSuccess);
    }

    void handleEndPacket(ErrorPacket packet, AutoTxOperation txOperation, boolean isSuccess) {
        session.resetMultiStatementStatus();
        handleEndPacketInner(packet, txOperation, isSuccess);
    }

    private byte[] getByteFromPacket(MySQLPacket packet) {
        if (packet instanceof OkPacket) {
            return ((OkPacket) packet).toBytes();
        } else if (packet instanceof ErrorPacket) {
            return ((ErrorPacket) packet).toBytes();
        } else {
            LOGGER.error("illegal use for this method");
            throw new IllegalStateException("illegal use for this method");
        }
    }

    private void handleEndPacketInner(MySQLPacket packet, AutoTxOperation txOperation, boolean isSuccess) {
        ServerConnection source = session.getSource();
        if (source.isAutocommit() && !source.isTxStart() && this.modifiedSQL && !this.session.isKilled()) {
            //Implicit Distributed Transaction,send commit or rollback automatically
            TransactionHandler handler = new AutoCommitHandler(session, getByteFromPacket(packet), rrs.getNodes(), errConnection);
            if (txOperation == AutoTxOperation.COMMIT) {
                session.checkBackupStatus();
                session.setBeginCommitTime();
                handler.commit();
            } else {
                handler.rollback();
            }
        } else {
            boolean inTransaction = !source.isAutocommit() || source.isTxStart();
            if (!inTransaction) {
                if (errConnection != null) {
                    for (BackendConnection errConn : errConnection) {
                        session.releaseConnection(errConn);
                    }
                }
            }

            // Explicit Distributed Transaction
            if (inTransaction && (AutoTxOperation.ROLLBACK == txOperation)) {
                source.setTxInterrupt("ROLLBACK");
            }
            session.setResponseTime(isSuccess);


            boolean multiStatementFlag = session.getIsMultiStatement().get();
            if (multiStatementFlag) {
                //maybe useless.
                packet.setPacketId(packetId);
            }
            session.getSource().write(getByteFromPacket(packet));
            session.multiStatementNextSql(multiStatementFlag);
        }
    }

    public void waitAllConnConnectorError() {
        while (connRrns.size() - 1 != errorConnsCnt) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    private void initDebugInfo() {
        if (LOGGER.isDebugEnabled()) {
            String info = DebugPauseUtil.getPauseInfo("MultiNodeQueryHandler.txt");
            if (info != null) {
                LOGGER.debug("Pause info of MultiNodeQueryHandler is " + info);
                String[] infos = info.split(";");
                dataNodePauseInfo = new HashMap<>(infos.length);
                boolean formatCorrect = true;
                for (String nodeInfo : infos) {
                    try {
                        String[] infoDetail = nodeInfo.split(":");
                        dataNodePauseInfo.put(infoDetail[0], Integer.valueOf(infoDetail[1]));
                    } catch (Throwable e) {
                        formatCorrect = false;
                        break;
                    }
                }
                if (!formatCorrect) {
                    dataNodePauseInfo.clear();
                }
            } else {
                dataNodePauseInfo = new HashMap<>(0);
            }
        } else {
            dataNodePauseInfo = new HashMap<>(0);
        }
    }

    private void pauseTime(BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
            Integer millis = dataNodePauseInfo.get(rNode.getName());
            if (millis == null) {
                return;
            }
            LOGGER.debug("datanode[" + rNode.getName() + "], which conn threadid[" + ((MySQLConnection) conn).getThreadId() + "] will sleep for " + millis + " milliseconds");
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.debug("datanode[" + rNode.getName() + "], which conn threadid[" + ((MySQLConnection) conn).getThreadId() + "] has slept for " + millis + " milliseconds");
        }
    }
}
