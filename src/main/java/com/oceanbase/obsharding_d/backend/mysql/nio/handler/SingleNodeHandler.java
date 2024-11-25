/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.backend.mysql.nio.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.LoadDataUtil;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.ServerConfig;
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
import com.oceanbase.obsharding_d.server.variables.OutputStateEnum;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.oceanbase.obsharding_d.statistic.stat.QueryResultDispatcher;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.oceanbase.obsharding_d.net.mysql.StatusFlags.SERVER_STATUS_CURSOR_EXISTS;

/**
 * @author mycat
 */
public class SingleNodeHandler implements ResponseHandler, LoadDataResponseHandler, ExecutableHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeHandler.class);
    protected final ReentrantLock lock = new ReentrantLock();
    protected final RouteResultsetNode node;
    protected final RouteResultset rrs;
    protected final NonBlockingSession session;

    // only one thread access at one time no need lock
    protected volatile ByteBuffer buffer;
    protected long netOutBytes;
    private long resultSize;
    long selectRows;
    protected int fieldCount;
    private List<FieldPacket> fieldPackets = new ArrayList<>();
    private volatile boolean connClosed = false;
    protected AtomicBoolean writeToClient = new AtomicBoolean(false);

    private final RequestScope requestScope;

    public SingleNodeHandler(RouteResultset rrs, NonBlockingSession session) {
        if (session == null) {
            throw new IllegalArgumentException("session is null!");
        }
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        this.node = rrs.getNodes()[0];
        this.rrs = rrs;
        this.session = session;
        this.requestScope = session.getShardingService().getRequestScope();
        TxnLogHelper.putTxnLog(session.getShardingService(), node);
    }

    @Override
    public void execute() throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "execute-for-sql");
        try {
            connClosed = false;
            RouteResultsetNode finalNode = null;
            if (session.getTargetCount() > 0) {
                BackendConnection conn = session.getTarget(node);
                if (conn == null && rrs.isGlobalTable() && rrs.getGlobalBackupNodes() != null) {
                    // read only trx for global table
                    for (String shardingNode : rrs.getGlobalBackupNodes()) {
                        RouteResultsetNode tmpNode = new RouteResultsetNode(shardingNode, rrs.getSqlType(), rrs.getStatement());
                        conn = session.getTarget(tmpNode);
                        if (conn != null) {
                            finalNode = tmpNode;
                            break;
                        }
                    }
                }
                node.setRunOnSlave(rrs.getRunOnSlave());
                if (session.tryExistsCon(conn, finalNode == null ? node : finalNode)) {
                    executeInExistsConnection(conn);
                    return;
                }
            }

            // create new connection
            node.setRunOnSlave(rrs.getRunOnSlave());
            ServerConfig conf = OBsharding_DServer.getInstance().getConfig();
            ShardingNode dn = conf.getShardingNodes().get(node.getName());
            dn.getConnection(dn.getDatabase(), session.getShardingService().isTxStart(), session.getShardingService().isAutocommit(), node, this, node);
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
    }

    protected void innerExecute(BackendConnection conn) {
        if (clearIfSessionClosed()) {
            return;
        }
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setSession(session);
        conn.getBackendService().execute(node, session.getShardingService(), !session.getShardingService().isInTransaction());
    }

    protected void executeInExistsConnection(BackendConnection conn) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "execute-in-exists-connection");
        try {
            TraceManager.crossThread(conn.getBackendService(), "backend-response-service", session.getShardingService());
            innerExecute(conn);
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
    }

    @Override
    public void clearAfterFailExecute() {
        recycleBuffer();
    }

    @Override
    public void writeRemainBuffer() {

    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        session.bindConnection(node, conn);
        innerExecute(conn);
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        RouteResultsetNode rrn = (RouteResultsetNode) attachment;
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(session.getShardingService().nextPacketId());
        errPacket.setErrNo(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
        String errMsg = "can't connect to shardingNode[" + rrn.getName() + "], due to " + e.getMessage();
        errPacket.setMessage(StringUtil.encode(errMsg, session.getShardingService().getCharset().getResults()));
        LOGGER.warn(errMsg);
        backConnectionErr(errPacket, null, false);
    }

    @Override
    public void errorResponse(byte[] data, @NotNull AbstractService service) {
        session.resetMultiStatementStatus();
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        err.setPacketId(session.getShardingService().nextPacketId());
        backConnectionErr(err, (MySQLResponseService) service, ((MySQLResponseService) service).syncAndExecute());
    }

    public void recycleBuffer() {
        lock.lock();
        try {
            if (buffer != null) {
                session.getSource().recycle(buffer);
                buffer = null;
            }
        } finally {
            lock.unlock();
        }
    }

    protected void backConnectionErr(ErrorPacket errPkg, @Nullable MySQLResponseService service, boolean syncFinished) {
        ShardingService shardingService = session.getShardingService();
        String errMsg = "errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
        LOGGER.info("execute sql err:{}, con:{}", errMsg, service);

        if (service != null && !service.isFakeClosed()) {
            if (service.getConnection().isClosed()) {
                if (service.getAttachment() != null) {
                    RouteResultsetNode rNode = (RouteResultsetNode) service.getAttachment();
                    session.getTargetMap().remove(rNode);
                }
            } else if (syncFinished) {
                session.releaseConnectionIfSafe(service, false);
            } else {
                service.getConnection().businessClose("unfinished sync");
                if (service.getAttachment() != null) {
                    RouteResultsetNode rNode = (RouteResultsetNode) service.getAttachment();
                    session.getTargetMap().remove(rNode);
                }
            }
        }

        if (errPkg.getErrNo() != ErrorCode.ER_DUP_ENTRY && errPkg.getErrNo() != ErrorCode.ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION) {
            shardingService.setTxInterrupt(errMsg);
        }
        lock.lock();
        try {
            if (writeToClient.compareAndSet(false, true)) {
                if (rrs.isLoadData()) {
                    session.getShardingService().getLoadDataInfileHandler().clear();
                }
                if (buffer != null) {
                    /* SELECT 9223372036854775807 + 1;    response: field_count, field, eof, err */
                    errPkg.write(buffer, shardingService);
                } else {
                    errPkg.write(shardingService.getConnection());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * insert/update/delete
     * <p>
     * okResponse():
     * read data, make an OKPacket, writeDirectly to writeQueue in FrontendConnection by ok.writeDirectly(source)
     */
    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-ok-packet");
        TraceManager.finishSpan(service, traceObject);
        this.netOutBytes += data.length;

        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            this.resultSize += data.length;
            ShardingService shardingService = session.getShardingService();
            OkPacket ok = new OkPacket();
            ok.read(data);
            if (rrs.isLoadData()) {
                ok.setPacketId(shardingService.nextPacketId()); // OK_PACKET
                shardingService.getLoadDataInfileHandler().clear();
                service.getConnection().updateLastReadTime();
                ok.setMessage(("Records: " + ok.getAffectedRows() + "  Deleted: 0  Skipped: 0  Warnings: " + ok.getWarningCount()).getBytes());
            } else {
                ok.setPacketId(shardingService.nextPacketId()); // OK_PACKET
                ok.setMessage(null);
            }
            session.setRowCount(ok.getAffectedRows());
            ok.setServerStatus(shardingService.isAutocommit() ? 2 : 1);
            shardingService.setLastInsertId(ok.getInsertId());
            session.setBackendResponseEndTime((MySQLResponseService) service);
            session.releaseConnectionIfSafe((MySQLResponseService) service, false);
            session.multiStatementPacket(ok);
            QueryResultDispatcher.doSqlStat(rrs, session, selectRows, netOutBytes, resultSize);
            if (OutputStateEnum.PREPARE.equals(requestScope.getOutputState())) {
                return;
            }
            if (rrs.isCallStatement() || writeToClient.compareAndSet(false, true)) {
                ok.write(shardingService.getConnection());
            }
        }
    }

    /**
     * select
     * <p>
     * writeDirectly EOF to Queue
     */
    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-rowEof-packet");
        TraceManager.finishSpan(service, traceObject);
        this.netOutBytes += eof.length;
        this.resultSize += eof.length;

        // if it's call statement,it will not release connection
        if (!rrs.isCallStatement()) {
            session.releaseConnectionIfSafe((MySQLResponseService) service, false);
        }
        if (OutputStateEnum.PREPARE.equals(requestScope.getOutputState())) {
            requestScope.getCurrentPreparedStatement().onPrepareOk(fieldCount);
            writeToClient.compareAndSet(false, true);
            return;
        }
        eof[3] = (byte) session.getShardingService().nextPacketId();

        EOFRowPacket eofRowPacket = new EOFRowPacket();
        eofRowPacket.read(eof);

        ShardingService shardingService = session.getShardingService();
        QueryResultDispatcher.doSqlStat(rrs, session, selectRows, netOutBytes, resultSize);
        if (requestScope.isUsingCursor()) {
            requestScope.getCurrentPreparedStatement().getCursorCache().done();
            session.getShardingService().writeDirectly(buffer, WriteFlags.QUERY_END, ResultFlag.EOF_ROW);
        }
        lock.lock();
        try {
            if (writeToClient.compareAndSet(false, true)) {
                if (!requestScope.isUsingCursor()) {
                    eofRowPacket.write(buffer, shardingService);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {
        this.netOutBytes += header.length;
        this.resultSize += header.length;
        for (byte[] field : fields) {
            this.netOutBytes += field.length;
            this.resultSize += field.length;
        }
        this.netOutBytes += eof.length;
        this.resultSize += eof.length;
        fieldCount = fields.size();
        if (OutputStateEnum.PREPARE.equals(requestScope.getOutputState())) {
            return;
        }

        header[3] = (byte) session.getShardingService().nextPacketId();

        ShardingService shardingService = session.getShardingService();
        lock.lock();
        try {
            if (!writeToClient.get()) {
                buffer = session.getSource().allocate();
                buffer = shardingService.writeToBuffer(header, buffer);
                for (int i = 0, len = fields.size(); i < len; ++i) {
                    byte[] field = fields.get(i);
                    field[3] = (byte) session.getShardingService().nextPacketId();

                    // save field
                    FieldPacket fieldPk = new FieldPacket();
                    fieldPk.read(field);
                    if (rrs.getSchema() != null) {
                        fieldPk.setDb(rrs.getSchema().getBytes());
                    }
                    if (rrs.getTableAlias() != null) {
                        fieldPk.setTable(rrs.getTableAlias().getBytes());
                    }
                    if (rrs.getTable() != null) {
                        fieldPk.setOrgTable(rrs.getTable().getBytes());
                    }
                    fieldPackets.add(fieldPk);

                    buffer = fieldPk.write(buffer, shardingService, false);
                }

                fieldCount = fieldPackets.size();
                if (requestScope.isUsingCursor()) {
                    requestScope.getCurrentPreparedStatement().initCursor(session, this, fields.size(), fieldPackets);
                }

                eof[3] = (byte) session.getShardingService().nextPacketId();
                if (requestScope.isUsingCursor()) {
                    byte statusFlag = 0;
                    statusFlag |= session.getShardingService().isAutocommit() ? 2 : 1;
                    statusFlag |= SERVER_STATUS_CURSOR_EXISTS;
                    eof[7] = statusFlag;
                }
                buffer = shardingService.writeToBuffer(eof, buffer);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        this.netOutBytes += row.length;
        this.resultSize += row.length;
        this.selectRows++;
        if (OutputStateEnum.PREPARE.equals(requestScope.getOutputState())) {
            return false;
        }
        lock.lock();
        try {
            if (!writeToClient.get()) {
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
                        buffer = binRowDataPk.write(buffer, session.getShardingService(), true);
                    }
                } else {
                    buffer = rowDataPk.write(buffer, session.getShardingService(), true);
                }
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-connection-closed");
        TraceManager.finishSpan(service, traceObject);
        if (connClosed) {
            return;
        }
        connClosed = true;
        LOGGER.warn("Backend connect Closed, reason is [" + reason + "], Connection info:" + service);
        reason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getConnection().getSchema() + "],threadID[" +
                ((MySQLResponseService) service).getConnection().getThreadId() + "]} was closed ,reason is [" + reason + "]";
        session.getSource().setSkipCheck(false);
        ErrorPacket err = new ErrorPacket();
        err.setPacketId((byte) session.getShardingService().nextPacketId());
        err.setErrNo(ErrorCode.ER_ERROR_ON_CLOSE);
        err.setMessage(StringUtil.encode(reason, session.getShardingService().getCharset().getResults()));
        this.backConnectionErr(err, (MySQLResponseService) service, true);
    }

    @Override
    public void requestDataResponse(byte[] data, @Nonnull MySQLResponseService service) {
        LoadDataUtil.requestFileDataResponse(data, service);
    }

    public boolean clearIfSessionClosed() {
        if (session.closed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("session closed without execution,clear resources " + session);
            }
            recycleBuffer();
            session.clearResources(true);
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "SingleNodeHandler [node=" + node + ", packetId=" + (byte) session.getShardingService().getPacketId().get() + "]";
    }

}
