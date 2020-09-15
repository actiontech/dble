/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.LoadDataUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.FlowControllerConfig;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.singleton.WriteQueueFlowController;
import com.actiontech.dble.statistic.stat.QueryResult;
import com.actiontech.dble.statistic.stat.QueryResultDispatcher;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public class SingleNodeHandler implements ResponseHandler, LoadDataResponseHandler, ExecutableHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeHandler.class);
    protected final ReentrantLock lock = new ReentrantLock();
    private final RouteResultsetNode node;
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


    public SingleNodeHandler(RouteResultset rrs, NonBlockingSession session) {
        this.rrs = rrs;
        this.node = rrs.getNodes()[0];
        if (node == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        if (session == null) {
            throw new IllegalArgumentException("session is null!");
        }
        this.session = session;
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
            ServerConfig conf = DbleServer.getInstance().getConfig();
            ShardingNode dn = conf.getShardingNodes().get(node.getName());
            dn.getConnection(dn.getDatabase(), session.getShardingService().isTxStart(), session.getShardingService().isAutocommit(), node, this, node);
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
    }

    protected void execute(BackendConnection conn) {
        if (session.closed()) {
            session.clearResources(true);
            recycleBuffer();
            return;
        }
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setSession(session);
        boolean isAutocommit = session.getShardingService().isAutocommit() && !session.getShardingService().isTxStart();
        if (!isAutocommit && node.isModifySQL()) {
            TxnLogHelper.putTxnLog(session.getShardingService(), node.getStatement());
        }
        conn.getBackendService().execute(node, session.getShardingService(), isAutocommit);
    }

    protected void executeInExistsConnection(BackendConnection conn) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "execute-in-exists-connection");
        try {
            TraceManager.crossThread(conn.getBackendService(), "backend-response-service", session.getShardingService());
            execute(conn);
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
        execute(conn);
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
    public void errorResponse(byte[] data, AbstractService service) {
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        err.setPacketId(session.getShardingService().nextPacketId());
        backConnectionErr(err, (MySQLResponseService) service, ((MySQLResponseService) service).syncAndExecute());
        session.resetMultiStatementStatus();
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

    protected void backConnectionErr(ErrorPacket errPkg, MySQLResponseService service, boolean syncFinished) {
        ShardingService shardingService = session.getShardingService();
        UserName errUser = shardingService.getUser();
        String errHost = shardingService.getConnection().getHost();
        int errPort = shardingService.getConnection().getLocalPort();

        String errMsg = " errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
        LOGGER.info("execute sql err :" + errMsg + " con:" + service +
                " frontend host:" + errHost + "/" + errPort + "/" + errUser);

        if (service != null) {
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

        if (errPkg.getErrNo() != ErrorCode.ER_DUP_ENTRY) {
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
                    buffer = shardingService.writeToBuffer(errPkg.toBytes(), buffer);
                    session.setResponseTime(false);
                    shardingService.writeDirectly(buffer);
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
    public void okResponse(byte[] data, AbstractService service) {
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
            } else {
                ok.setPacketId(shardingService.nextPacketId()); // OK_PACKET
            }
            session.setRowCount(ok.getAffectedRows());
            ok.setMessage(null);
            ok.setServerStatus(shardingService.isAutocommit() ? 2 : 1);
            shardingService.setLastInsertId(ok.getInsertId());
            session.setBackendResponseEndTime((MySQLResponseService) service);
            session.releaseConnectionIfSafe((MySQLResponseService) service, false);
            session.setResponseTime(true);
            session.multiStatementPacket(ok);
            doSqlStat();
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
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-rowEof-packet");
        TraceManager.finishSpan(service, traceObject);
        this.netOutBytes += eof.length;
        this.resultSize += eof.length;
        // if it's call statement,it will not release connection
        if (!rrs.isCallStatement()) {
            session.releaseConnectionIfSafe((MySQLResponseService) service, false);
        }

        eof[3] = (byte) session.getShardingService().nextPacketId();

        EOFRowPacket eofRowPacket = new EOFRowPacket();
        eofRowPacket.read(eof);

        ShardingService shardingService = session.getShardingService();
        session.setResponseTime(true);
        doSqlStat();
        lock.lock();
        try {
            if (writeToClient.compareAndSet(false, true)) {
                eofRowPacket.write(buffer, shardingService);
            }
        } finally {
            lock.unlock();
        }
    }

    protected void doSqlStat() {
        if (SystemConfig.getInstance().getUseSqlStat() == 1) {
            long netInBytes = 0;
            if (rrs.getStatement() != null) {
                netInBytes = rrs.getStatement().getBytes().length;
            }
            QueryResult queryResult = new QueryResult(session.getShardingService().getUser(), rrs.getSqlType(), rrs.getStatement(), selectRows,
                    netInBytes, netOutBytes, session.getQueryStartTime(), System.currentTimeMillis(), resultSize);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("try to record sql:" + rrs.getStatement());
            }
            QueryResultDispatcher.dispatchQuery(queryResult);
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, AbstractService service) {
        this.netOutBytes += header.length;
        this.resultSize += header.length;
        for (byte[] field : fields) {
            this.netOutBytes += field.length;
            this.resultSize += field.length;
        }
        this.netOutBytes += eof.length;
        this.resultSize += eof.length;

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

                eof[3] = (byte) session.getShardingService().nextPacketId();
                buffer = shardingService.writeToBuffer(eof, buffer);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        this.netOutBytes += row.length;
        this.resultSize += row.length;
        this.selectRows++;
        lock.lock();
        try {
            if (!writeToClient.get()) {
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
                    buffer = binRowDataPk.write(buffer, session.getShardingService(), true);
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
    public void connectionClose(AbstractService service, String reason) {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(service, "get-connection-closed");
        TraceManager.finishSpan(service, traceObject);
        if (connClosed) {
            return;
        }
        connClosed = true;
        LOGGER.warn("Backend connect Closed, reason is [" + reason + "], Connection info:" + service);
        reason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getConnection().getSchema() + "],threadID[" +
                ((MySQLResponseService) service).getConnection().getThreadId() + "]} was closed ,reason is [" + reason + "]";
        ErrorPacket err = new ErrorPacket();
        err.setPacketId((byte) session.getShardingService().nextPacketId());
        err.setErrNo(ErrorCode.ER_ERROR_ON_CLOSE);
        err.setMessage(StringUtil.encode(reason, session.getShardingService().getCharset().getResults()));
        this.backConnectionErr(err, (MySQLResponseService) service, true);
    }

    @Override
    public void requestDataResponse(byte[] data, MySQLResponseService service) {
        LoadDataUtil.requestFileDataResponse(data, service);
    }

    @Override
    public String toString() {
        return "SingleNodeHandler [node=" + node + ", packetId=" + (byte) session.getShardingService().getPacketId().get() + "]";
    }

}
