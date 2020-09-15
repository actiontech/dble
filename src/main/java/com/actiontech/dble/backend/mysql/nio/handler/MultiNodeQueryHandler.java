/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.LoadDataUtil;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AutoCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AutoTxOperation;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.FlowControllerConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.singleton.WriteQueueFlowController;
import com.actiontech.dble.statistic.stat.QueryResult;
import com.actiontech.dble.statistic.stat.QueryResultDispatcher;
import com.actiontech.dble.util.DebugUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author mycat
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements LoadDataResponseHandler, ExecutableHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeQueryHandler.class);
    protected final RouteResultset rrs;
    protected final boolean sessionAutocommit;
    private long affectedRows;
    long selectRows;
    protected List<MySQLResponseService> errConnection;
    private long netOutBytes;
    private long resultSize;
    protected ErrorPacket err;
    protected int fieldCount = 0;
    volatile boolean fieldsReturned;
    private long insertId;
    private List<FieldPacket> fieldPackets = new ArrayList<>();
    protected volatile ByteBuffer byteBuffer;
    protected Set<MySQLResponseService> closedConnSet;
    private final boolean modifiedSQL;
    protected Set<RouteResultsetNode> connRrns = new ConcurrentSkipListSet<>();
    private Map<String, Integer> shardingNodePauseInfo; // only for debug

    public MultiNodeQueryHandler(RouteResultset rrs, NonBlockingSession session) {
        super(session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute multi node query " + rrs.getStatement());
        }
        this.rrs = rrs;
        if (ServerParse.SELECT == rrs.getSqlType()) {
            byteBuffer = session.getSource().allocate();
        }
        this.sessionAutocommit = session.getShardingService().isAutocommit();
        this.modifiedSQL = rrs.getNodes()[0].isModifySQL();
        initDebugInfo();
    }

    @Override
    protected void reset() {
        super.reset();
        connRrns.clear();
        this.netOutBytes = 0;
        this.resultSize = 0;
    }

    public void writeRemainBuffer() {
        lock.lock();
        try {
            if (byteBuffer != null) {
                session.getSource().write(byteBuffer);
                byteBuffer = null;
            }
        } finally {
            lock.unlock();
        }
    }

    public NonBlockingSession getSession() {
        return session;
    }

    @Override
    public void execute() throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "execute-for-sql");
        try {
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
                TxnLogHelper.putTxnLog(session.getShardingService(), sb.toString());
            }

            for (final RouteResultsetNode node : rrs.getNodes()) {
                BackendConnection conn = session.getTarget(node);
                if (session.tryExistsCon(conn, node)) {
                    node.setRunOnSlave(rrs.getRunOnSlave());
                    executeInExistsConnection(conn, node);
                } else {
                    connRrns.add(node);
                    // create new connection
                    node.setRunOnSlave(rrs.getRunOnSlave());
                    ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
                    dn.getConnection(dn.getDatabase(), session.getShardingService().isTxStart(), sessionAutocommit, node, this, node);
                }
            }
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
    }

    protected void executeInExistsConnection(BackendConnection conn, RouteResultsetNode node) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "execute-in-exists-connection");
        try {
            TraceManager.crossThread(conn.getBackendService(), "backend-response-service", session.getShardingService());
            innerExecute(conn, node);
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session)) {
            return;
        }
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setSession(session);
        conn.getBackendService().executeMultiNode(node, session.getShardingService(), sessionAutocommit && !session.getShardingService().isTxStart() && !node.isModifySQL());
    }

    @Override
    public void clearAfterFailExecute() {
        if (!session.getShardingService().isAutocommit() || session.getShardingService().isTxStart()) {
            session.getShardingService().setTxInterrupt("ROLLBACK");
        }
        waitAllConnConnectorError();
        cleanBuffer();

        session.forceClose("other node prepare conns failed");
    }

    void cleanBuffer() {
        lock.lock();
        try {
            if (byteBuffer != null) {
                session.getSource().recycle(byteBuffer);
                byteBuffer = null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        pauseTime((MySQLResponseService) service);
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-connection-closed");
        TraceManager.finishSpan(service, traceObject);
        if (checkClosedConn((MySQLResponseService) service)) {
            return;
        }
        LOGGER.warn("backend connect " + reason + ", conn info:" + service);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        reason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getConnection().getSchema() + "],threadID[" +
                ((MySQLResponseService) service).getConnection().getThreadId() + "]} was closed ,reason is [" + reason + "]";
        errPacket.setMessage(StringUtil.encode(reason, session.getShardingService().getCharset().getResults()));
        err = errPacket;
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            RouteResultsetNode rNode = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
            unResponseRrns.remove(rNode);
            session.getTargetMap().remove(rNode);
            ((MySQLResponseService) service).setResponseHandler(null);
            executeError((MySQLResponseService) service);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        RouteResultsetNode rrn = (RouteResultsetNode) attachment;
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setErrNo(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
        String errMsg = "can't connect to shardingNode[" + rrn.getName() + "], due to " + e.getMessage();
        errPacket.setMessage(StringUtil.encode(errMsg, session.getShardingService().getCharset().getResults()));
        LOGGER.warn(errMsg);
        err = errPacket;
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            errorConnsCnt++;
            executeError(null);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection connection) {
        final RouteResultsetNode node = (RouteResultsetNode) connection.getBackendService().getAttachment();
        session.bindConnection(node, connection);
        connRrns.remove(node);
        innerExecute(connection, node);
    }

    @Override
    public void errorResponse(byte[] data, AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-sql-execute-error");
        TraceManager.finishSpan(service, traceObject);
        pauseTime((MySQLResponseService) service);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(data);
        session.resetMultiStatementStatus();
        lock.lock();
        try {
            if (!isFail()) {
                err = errPacket;
                setFail(new String(err.getMessage()));
            }
            if (errConnection == null) {
                errConnection = new ArrayList<>();
            }
            errConnection.add((MySQLResponseService) service);
            if (decrementToZero((MySQLResponseService) service)) {
                if (session.closed()) {
                    cleanBuffer();
                } else if (byteBuffer != null) {
                    session.getSource().write(byteBuffer);
                }
                //just for normal error
                ErrorPacket errorPacket = createErrPkg(this.error, err.getErrNo());
                handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void okResponse(byte[] data, AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-ok-response");
        TraceManager.finishSpan(service, traceObject);
        this.netOutBytes += data.length;
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:" + executeResponse + " from " + service);
        }
        if (executeResponse) {
            pauseTime((MySQLResponseService) service);
            this.resultSize += data.length;
            session.setBackendResponseEndTime((MySQLResponseService) service);
            ShardingService shardingService = session.getShardingService();
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
                if (!decrementToZero((MySQLResponseService) service))
                    return;
                if (isFail()) {
                    ErrorPacket errorPacket = createErrPkg(this.error, err.getErrNo());
                    handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
                    return;
                }
                ok.setPacketId(session.getShardingService().nextPacketId()); // OK_PACKET
                if (rrs.isLoadData()) {
                    ok.setMessage(("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: 0").getBytes());
                    shardingService.getLoadDataInfileHandler().clear();
                } else {
                    ok.setMessage(null);
                }

                ok.setAffectedRows(affectedRows);
                session.setRowCount(affectedRows);
                ok.setServerStatus(shardingService.isAutocommit() ? 2 : 1);
                if (insertId > 0) {
                    ok.setInsertId(insertId);
                    shardingService.setLastInsertId(insertId);
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
                                 boolean isLeft, AbstractService service) {
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
    public void rowEofResponse(final byte[] eof, boolean isLeft, AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-rowEof-response");
        TraceManager.finishSpan(service, traceObject);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("on row end response " + service);
        }

        this.netOutBytes += eof.length;
        if (errorResponse.get()) {
            return;
        }
        RouteResultsetNode rNode = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
        final ShardingService source = session.getShardingService();
        if (!rrs.isCallStatement()) {
            if (clearIfSessionClosed(session)) {
                cleanBuffer();
                return;
            } else {
                session.releaseConnectionIfSafe((MySQLResponseService) service, false);
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
                    if (this.sessionAutocommit && !session.getShardingService().isTxStart() && !session.getShardingService().isLocked()) { // clear all connections
                        session.releaseConnections(false);
                    }

                    if (this.isFail()) {
                        session.setResponseTime(false);
                        session.resetMultiStatementStatus();
                        if (session.closed()) {
                            cleanBuffer();
                        } else if (byteBuffer != null) {
                            session.getSource().write(byteBuffer);
                        }
                        ErrorPacket errorPacket = createErrPkg(this.error, err.getErrNo());
                        handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
                        return;
                    }
                }

                if (session.closed()) {
                    cleanBuffer();
                } else {
                    writeEofResult(eof, source);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketNull, boolean isLeft, AbstractService service) {
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

            if (rrs.getLimitSize() >= 0) {
                if (selectRows <= rrs.getLimitStart() ||
                        (selectRows > (rrs.getLimitStart() < 0 ? 0 : rrs.getLimitStart()) + rrs.getLimitSize())) {
                    return false;
                }
            }
            this.resultSize += row.length;

            if (!errorResponse.get() && byteBuffer != null) {
                FlowControllerConfig fconfig = WriteQueueFlowController.getFlowCotrollerConfig();
                if (fconfig.isEnableFlowControl() &&
                        session.getSource().getWriteQueue().size() > fconfig.getStart()) {
                    session.getSource().startFlowControl();
                }

                RowDataPacket rowDataPk = new RowDataPacket(fieldCount);
                row[3] = (byte) session.getShardingService().nextPacketId();
                rowDataPk.read(row);
                if (session.isPrepared()) {
                    BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
                    binRowDataPk.read(fieldPackets, rowDataPk);
                    binRowDataPk.setPacketId(rowDataPk.getPacketId());
                    byteBuffer = binRowDataPk.write(byteBuffer, session.getShardingService(), true);
                } else {
                    byteBuffer = rowDataPk.write(byteBuffer, session.getShardingService(), true);
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
        cleanBuffer();
    }

    @Override
    public void requestDataResponse(byte[] data, MySQLResponseService service) {
        LoadDataUtil.requestFileDataResponse(data, service);
    }


    private void executeError(MySQLResponseService service) {
        if (!isFail()) {
            setFail(new String(err.getMessage()));
        }
        if (errConnection == null) {
            errConnection = new ArrayList<>();
        }
        if (service != null) {
            errConnection.add(service);
            if (service.getConnection().isClosed() && (!session.getShardingService().isAutocommit() || session.getShardingService().isTxStart())) {
                session.getShardingService().setTxInterrupt(error);
            }
        }

        if (canResponse()) {
            if (byteBuffer == null) {
                ErrorPacket errorPacket = createErrPkg(this.error, err.getErrNo());
                handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
            } else if (session.closed()) {
                cleanBuffer();
            } else {
                ErrorPacket errorPacket = createErrPkg(this.error, err.getErrNo());
                session.getSource().write(byteBuffer);
                handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
            }
        }
    }

    private void writeEofResult(byte[] eof, ShardingService source) {
        if (byteBuffer == null) {
            return;
        }
        EOFRowPacket eofRowPacket = new EOFRowPacket();
        eofRowPacket.read(eof);
        eofRowPacket.setPacketId((byte) session.getShardingService().nextPacketId());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("last packet id:" + (byte) session.getShardingService().getPacketId().get());
        }
        session.setResponseTime(true);
        doSqlStat();
        eofRowPacket.write(byteBuffer, source);
    }

    void doSqlStat() {
        if (SystemConfig.getInstance().getUseSqlStat() == 1) {
            long netInBytes = 0;
            if (rrs != null && rrs.getStatement() != null) {
                netInBytes += rrs.getStatement().getBytes().length;
            }
            assert rrs != null;
            QueryResult queryResult = new QueryResult(session.getShardingService().getUser(), rrs.getSqlType(),
                    rrs.getStatement(), selectRows, netInBytes, netOutBytes, session.getQueryStartTime(), System.currentTimeMillis(), resultSize);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try to record sql:" + rrs.getStatement());
            }
            QueryResultDispatcher.dispatchQuery(queryResult);
        }
    }

    private void executeFieldEof(byte[] header, List<byte[]> fields, byte[] eof) {
        if (byteBuffer == null) {
            return;
        }
        ShardingService service = session.getShardingService();
        fieldCount = fields.size();
        header[3] = (byte) session.getShardingService().nextPacketId();
        byteBuffer = service.writeToBuffer(header, byteBuffer);

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
                fieldPkg.setPacketId(session.getShardingService().nextPacketId());
                byteBuffer = fieldPkg.write(byteBuffer, service, false);
            }
            eof[3] = (byte) session.getShardingService().nextPacketId();
            byteBuffer = service.writeToBuffer(eof, byteBuffer);
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

    private boolean checkClosedConn(MySQLResponseService service) {
        lock.lock();
        try {
            if (closedConnSet == null) {
                closedConnSet = new HashSet<>(1);
                closedConnSet.add(service);
            } else {
                if (closedConnSet.contains(service)) {
                    return true;
                }
                closedConnSet.add(service);
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    void handleEndPacket(MySQLPacket packet, AutoTxOperation txOperation, boolean isSuccess) {
        ShardingService service = session.getShardingService();
        if (rrs.isLoadData()) {
            service.getLoadDataInfileHandler().clear();
        }

        if (service.isAutocommit() && !service.isTxStart() && this.modifiedSQL && !this.session.isKilled()) {
            //Implicit Distributed Transaction,send commit or rollback automatically
            TransactionHandler handler = new AutoCommitHandler(session, packet, rrs.getNodes(), errConnection);
            if (txOperation == AutoTxOperation.COMMIT) {
                session.checkBackupStatus();
                session.setBeginCommitTime();
                handler.commit();
            } else {
                handler.rollback();
            }
        } else {
            boolean inTransaction = !service.isAutocommit() || service.isTxStart();
            if (!inTransaction) {
                if (errConnection != null) {
                    for (MySQLResponseService servicex : errConnection) {
                        session.releaseConnection(servicex.getConnection());
                    }
                }
            }

            // Explicit Distributed Transaction
            if (inTransaction && (AutoTxOperation.ROLLBACK == txOperation)) {
                service.setTxInterrupt("ROLLBACK");
            }
            session.setResponseTime(isSuccess);

            packet.write(session.getSource());
        }
    }

    private void waitAllConnConnectorError() {
        while (connRrns.size() - 1 != errorConnsCnt) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    private void initDebugInfo() {
        if (LOGGER.isDebugEnabled()) {
            String info = DebugUtil.getDebugInfo("MultiNodeQueryHandler.txt");
            if (info != null) {
                LOGGER.debug("Pause info of MultiNodeQueryHandler is " + info);
                String[] infos = info.split(";");
                shardingNodePauseInfo = new HashMap<>(infos.length);
                boolean formatCorrect = true;
                for (String nodeInfo : infos) {
                    try {
                        String[] infoDetail = nodeInfo.split(":");
                        shardingNodePauseInfo.put(infoDetail[0], Integer.valueOf(infoDetail[1]));
                    } catch (Throwable e) {
                        formatCorrect = false;
                        break;
                    }
                }
                if (!formatCorrect) {
                    shardingNodePauseInfo.clear();
                }
            } else {
                shardingNodePauseInfo = new HashMap<>(0);
            }
        } else {
            shardingNodePauseInfo = new HashMap<>(0);
        }
    }

    private void pauseTime(MySQLResponseService service) {
        if (LOGGER.isDebugEnabled()) {
            RouteResultsetNode rNode = (RouteResultsetNode) service.getAttachment();
            Integer millis = shardingNodePauseInfo.get(rNode.getName());
            if (millis == null) {
                return;
            }
            LOGGER.debug("shardingnode[" + rNode.getName() + "], which conn threadid[" + service.getConnection().getThreadId() + "] will sleep for " + millis + " milliseconds");
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.debug("shardingnode[" + rNode.getName() + "], which conn threadid[" + service.getConnection().getThreadId() + "] has slept for " + millis + " milliseconds");
        }
    }
}
