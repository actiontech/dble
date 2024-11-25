/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.backend.mysql.nio.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.LoadDataUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.AutoCommitHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.AutoTxOperation;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionHandler;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.log.transaction.TxnLogHelper;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.*;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.ResultFlag;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.server.RequestScope;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.variables.OutputStateEnum;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.statistic.stat.QueryResultDispatcher;
import com.oceanbase.obsharding_d.util.DebugUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.oceanbase.obsharding_d.net.mysql.StatusFlags.SERVER_STATUS_CURSOR_EXISTS;

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
    protected long netOutBytes;
    protected long resultSize;
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
    private final RequestScope requestScope;
    private int loadDataErrorCount;
    private int readOnlyErrorCount;

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
        this.sessionAutocommit = session.getShardingService().isAutocommit();
        this.modifiedSQL = rrs.getNodes()[0].isModifySQL();
        initDebugInfo();
        requestScope = session.getShardingService().getRequestScope();
        TxnLogHelper.putTxnLog(session.getShardingService(), rrs);
    }

    @Override
    protected void reset() {
        super.reset();
        connRrns.clear();
        this.netOutBytes = 0;
        this.resultSize = 0;
        loadDataErrorCount = 0;
        this.readOnlyErrorCount = 0;
    }

    public void writeRemainBuffer() {
        lock.lock();
        try {
            if (byteBuffer != null) {
                session.getShardingService().writeDirectly(byteBuffer, WriteFlags.PART);
                byteBuffer = null;
            }
        } finally {
            lock.unlock();
        }
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
            for (RouteResultsetNode node : rrs.getNodes()) {
                unResponseRrns.add(node);
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
                    ShardingNode dn = OBsharding_DServer.getInstance().getConfig().getShardingNodes().get(node.getName());
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
        conn.getBackendService().setComplexQuery(complexQuery);
        conn.getBackendService().executeMultiNode(node, session.getShardingService(), sessionAutocommit && !session.getShardingService().isTxStart() && !node.isModifySQL());
    }

    @Override
    public void clearAfterFailExecute() {
        if (session.getShardingService().isInTransaction()) {
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
    public void connectionClose(@NotNull AbstractService service, String reason) {
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
            session.getSource().setSkipCheck(false);
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
    public void errorResponse(byte[] data, @NotNull AbstractService service) {
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
            if (errPacket.getErrNo() == ErrorCode.ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION) {
                readOnlyErrorCount++;
            }
            if (decrementToZero((MySQLResponseService) service)) {
                if (session.closed()) {
                    cleanBuffer();
                } else if (byteBuffer != null) {
                    session.getShardingService().writeDirectly(byteBuffer, WriteFlags.PART);
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
    public void okResponse(byte[] data, @NotNull AbstractService service) {
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
                    loadDataErrorCount += ok.getWarningCount();
                } else {
                    affectedRows = ok.getAffectedRows();
                    loadDataErrorCount = ok.getWarningCount();
                }
                if (ok.getInsertId() > 0) {
                    insertId = (insertId == 0) ? ok.getInsertId() : Math.min(insertId, ok.getInsertId());
                }
                if (rrs.isLoadData()) {
                    service.getConnection().updateLastReadTime();
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
                    ok.setMessage(("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: " + loadDataErrorCount).getBytes());
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
                QueryResultDispatcher.doSqlStat(rrs, session, selectRows, netOutBytes, resultSize);
                if (OutputStateEnum.PREPARE.equals(requestScope.getOutputState())) {
                    return;
                }
                handleEndPacket(ok, AutoTxOperation.COMMIT, true);
            } finally {
                lock.unlock();
            }
        }
    }


    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {
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
            if (isFail() && firstResponsed) {
                cleanBuffer();
                return;
            }
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
    public void rowEofResponse(final byte[] eof, boolean isLeft, @NotNull AbstractService service) {
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
                if (OutputStateEnum.PREPARE.equals(requestScope.getOutputState())) {
                    recycle();
                    requestScope.getCurrentPreparedStatement().onPrepareOk(fieldCount);
                    return;
                }
                if (requestScope.isUsingCursor()) {
                    recycle();
                    requestScope.getCurrentPreparedStatement().getCursorCache().done();
                    session.getShardingService().writeDirectly(byteBuffer, WriteFlags.QUERY_END, ResultFlag.EOF_ROW);
                    return;
                }
                this.resultSize += eof.length;
                if (!rrs.isCallStatement()) {
                    if (this.sessionAutocommit && !session.getShardingService().isTxStart() && !session.getShardingService().isLockTable()) { // clear all connections
                        session.releaseConnections(false);
                    }

                    if (this.isFail()) {
                        session.resetMultiStatementStatus();
                        if (session.closed()) {
                            cleanBuffer();
                        } else if (byteBuffer != null) {
                            session.getShardingService().writeDirectly(byteBuffer, WriteFlags.PART);
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

    private void recycle() {
        if (!rrs.isCallStatement()) {
            if (this.sessionAutocommit && !session.getShardingService().isTxStart() && !session.getShardingService().isLockTable()) { // clear all connections
                session.releaseConnections(false);
            }
        }
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketNull, boolean isLeft, @NotNull AbstractService service) {
        this.netOutBytes += row.length;
        if (OutputStateEnum.PREPARE.equals(requestScope.getOutputState())) {
            return false;
        }
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
                RowDataPacket rowDataPk = new RowDataPacket(fieldCount);
                if (!requestScope.isUsingCursor()) {
                    row[3] = (byte) session.getShardingService().nextPacketId();
                }
                rowDataPk.read(row);
                if (requestScope.isPrepared()) {
                    if (requestScope.isUsingCursor()) {
                        requestScope.getCurrentPreparedStatement().getCursorCache().add(rowDataPk);
                    } else {
                        BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
                        binRowDataPk.read(fieldPackets, rowDataPk);
                        binRowDataPk.setPacketId(rowDataPk.getPacketId());
                        byteBuffer = binRowDataPk.write(byteBuffer, session.getShardingService(), true);
                    }
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
    public void requestDataResponse(byte[] data, @Nonnull MySQLResponseService service) {
        LoadDataUtil.requestFileDataResponse(data, service);
    }


    private void executeError(@Nullable MySQLResponseService service) {
        if (!isFail()) {
            setFail(new String(err.getMessage()));
        }
        if (errConnection == null) {
            errConnection = new ArrayList<>();
        }
        if (service != null && !service.isFakeClosed()) {
            errConnection.add(service);
            if (service.getConnection().isClosed() && session.getShardingService().isInTransaction()) {
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
                session.getShardingService().writeDirectly(byteBuffer, WriteFlags.QUERY_END, ResultFlag.ERROR);
                handleEndPacket(errorPacket, AutoTxOperation.ROLLBACK, false);
            }
        }
    }

    private void writeEofResult(byte[] eof, ShardingService source) {
        if (byteBuffer == null) {
            return;
        }
        if (requestScope.isUsingCursor()) {
            return;
        }
        EOFRowPacket eofRowPacket = new EOFRowPacket();
        eofRowPacket.read(eof);
        eofRowPacket.setPacketId((byte) session.getShardingService().nextPacketId());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("last packet id:" + (byte) session.getShardingService().getPacketId().get());
        }
        QueryResultDispatcher.doSqlStat(rrs, session, selectRows, netOutBytes, resultSize);
        eofRowPacket.write(byteBuffer, source);
    }

    private void executeFieldEof(byte[] header, List<byte[]> fields, byte[] eof) {
        if (byteBuffer == null) {
            return;
        }
        ShardingService service = session.getShardingService();
        fieldCount = fields.size();
        if (OutputStateEnum.PREPARE.equals(requestScope.getOutputState())) {
            return;
        }
        header[3] = (byte) service.nextPacketId();
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
            if (requestScope.isUsingCursor()) {
                requestScope.getCurrentPreparedStatement().initCursor(session, this, fields.size(), fieldPackets);
            }

            eof[3] = (byte) session.getShardingService().nextPacketId();
            if (requestScope.isUsingCursor()) {
                byte statusFlag = 0;
                statusFlag |= service.getSession2().getShardingService().isAutocommit() ? 2 : 1;
                statusFlag |= SERVER_STATUS_CURSOR_EXISTS;
                eof[7] = statusFlag;
            }
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

        if (errorConnsCnt == rrs.getNodes().length) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("all nodes can't connect.");
            }
            packet.write(session.getSource());
            return;
        }

        if (readOnlyErrorCount == rrs.getNodes().length) {
            packet.write(session.getSource());
            return;
        }

        if (!service.isInTransaction() && this.modifiedSQL && !this.session.isKilled()) {
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
            boolean inTransaction = service.isInTransaction();
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
