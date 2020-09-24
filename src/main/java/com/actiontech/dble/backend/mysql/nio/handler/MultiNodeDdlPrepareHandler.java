/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.cluster.values.DDLTraceInfo;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.util.RouteResultCopy;
import com.actiontech.dble.server.NonBlockingSession;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.trace.TraceResult;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.DDLTraceManager;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author guoji.ma@gmail.com
 */
public class MultiNodeDdlPrepareHandler extends MultiNodeHandler implements ExecutableHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeDdlPrepareHandler.class);

    private static final String STMT = "select 1";
    private final RouteResultset rrs;
    private final RouteResultset oriRrs;
    private final boolean sessionAutocommit;
    private final MultiNodeDDLExecuteHandler handler;
    private ErrorPacket err;
    private Set<MySQLResponseService> closedConnSet;
    private volatile boolean finishedTest = false;
    private AtomicBoolean releaseDDLLock = new AtomicBoolean(false);

    public MultiNodeDdlPrepareHandler(RouteResultset rrs, NonBlockingSession session) {
        super(session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute multiNode query " + rrs.getStatement());
        }

        this.rrs = RouteResultCopy.rrCopy(rrs, ServerParse.DDL, STMT);
        this.sessionAutocommit = session.getShardingService().isAutocommit();

        this.oriRrs = rrs;
        this.handler = new MultiNodeDDLExecuteHandler(rrs, session);
    }

    @Override
    protected void reset() {
        super.reset();
    }

    public NonBlockingSession getSession() {
        return session;
    }

    @Override
    public void execute() throws Exception {
        TraceManager.TraceObject traceObject = TraceManager.serviceTrace(session.getShardingService(), "execute-for-ddl-prepare");
        try {
            lock.lock();
            try {
                this.reset();
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

            DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.CONN_TEST_START, session.getShardingService());

            for (final RouteResultsetNode node : rrs.getNodes()) {
                BackendConnection conn = session.getTarget(node);
                if (session.tryExistsCon(conn, node)) {
                    node.setRunOnSlave(rrs.getRunOnSlave());
                    existsConnectionExecute(conn.getBackendService(), node);
                } else {
                    // create new connection
                    node.setRunOnSlave(rrs.getRunOnSlave());
                    ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
                    dn.getConnection(dn.getDatabase(), true, sessionAutocommit, node, this, node);
                }
            }
        } finally {
            TraceManager.finishSpan(session.getShardingService(), traceObject);
        }
    }

    private void existsConnectionExecute(MySQLResponseService responseService, RouteResultsetNode node) {
        TraceManager.crossThread(responseService, "execute-in-exists-connection", session.getShardingService());
        innerExecute(responseService, node);
    }

    private void innerExecute(MySQLResponseService responseService, RouteResultsetNode node) {
        if (clearIfSessionClosed()) {
            return;
        }
        responseService.setResponseHandler(this);
        responseService.setSession(session);
        responseService.setTesting(true);
        responseService.setComplexQuery(true);
        responseService.execute(node, session.getShardingService(), sessionAutocommit && !session.getShardingService().isTxStart());
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(),
                (MySQLResponseService) service, DDLTraceInfo.DDLConnectionStatus.TEST_CONN_CLOSE);
        if (checkClosedConn(((MySQLResponseService) service).getConnection())) {
            return;
        }
        LOGGER.info("backend connect" + reason);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(reason, session.getShardingService().getCharset().getResults()));
        err = errPacket;

        lock.lock();
        try {
            RouteResultsetNode rNode = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
            unResponseRrns.remove(rNode);
            executeConnError();
        } finally {
            lock.unlock();
        }
    }

    private boolean checkClosedConn(BackendConnection conn) {
        lock.lock();
        try {
            if (finishedTest) {
                return true;
            }
            if (closedConnSet == null) {
                closedConnSet = new HashSet<>(1);
                closedConnSet.add(conn.getBackendService());
            } else {
                if (closedConnSet.contains(conn.getBackendService())) {
                    return true;
                }
                closedConnSet.add(conn.getBackendService());
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    private void executeConnError() {
        if (!isFail()) {
            setFail(new String(err.getMessage()));
        }
        if (canResponse() && errorResponse.compareAndSet(false, true)) {
            if (releaseDDLLock.compareAndSet(false, true)) {
                session.handleSpecial(oriRrs, false, null);
            }
            handleRollbackPacket(err.toBytes(), "DDL prepared failed");
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        DDLTraceManager.getInstance().updateRouteNodeStatus(session.getShardingService(),
                (RouteResultsetNode) attachment, DDLTraceInfo.DDLConnectionStatus.TEST_CONN_ERROR);

        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_ABORTING_CONNECTION);
        errPacket.setMessage(StringUtil.encode(e.toString(), session.getShardingService().getCharset().getResults()));
        err = errPacket;

        lock.lock();
        try {
            errorConnsCnt++;
            executeConnError();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionAcquired(final BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getBackendService().getAttachment();
        session.bindConnection(node, conn);
        DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(),
                conn.getBackendService(), DDLTraceInfo.DDLConnectionStatus.CONN_TEST_START);
        innerExecute(conn.getBackendService(), node);
    }


    @Override
    public void errorResponse(byte[] data, AbstractService service) {
        DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(),
                (MySQLResponseService) service, DDLTraceInfo.DDLConnectionStatus.CONN_TEST_RESULT_ERROR);
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(data);
        errPacket.setPacketId(1);
        err = errPacket;
        LOGGER.info("ddl do select 1 errorResponse:", err.getMessage());
        lock.lock();
        try {
            if (!isFail()) {
                setFail(new String(errPacket.getMessage()));
            }
            if (decrementToZero((MySQLResponseService) service) && errorResponse.compareAndSet(false, true)) {
                session.handleSpecial(oriRrs, false, null);
                handleRollbackPacket(err.toBytes(), "DDL prepared failed");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void okResponse(byte[] data, AbstractService service) {
        if (!((MySQLResponseService) service).syncAndExecute()) {
            LOGGER.debug("MultiNodeDdlPrepareHandler syncAndExecute!");
        } else {
            LOGGER.debug("MultiNodeDdlPrepareHandler syncAndExecute finished!");
        }
    }

    @Override
    public void rowEofResponse(final byte[] eof, boolean isLeft, AbstractService service) {
        MySQLResponseService responseService = (MySQLResponseService) service;
        DDLTraceManager.getInstance().updateConnectionStatus(session.getShardingService(),
                responseService, DDLTraceInfo.DDLConnectionStatus.CONN_TEST_SUCCESS);
        final ShardingService shardingService = session.getShardingService();
        if (clearIfSessionClosed()) {
            return;
        }

        lock.lock();
        try {
            responseService.setTesting(false);
            if (!decrementToZero(responseService))
                return;

            if (this.isFail()) {
                if (errorResponse.compareAndSet(false, true)) {
                    session.handleSpecial(oriRrs, false, null);
                    handleRollbackPacket(err.toBytes(), "DDL prepared failed");
                }
            } else {
                try {
                    DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.CONN_TEST_END, shardingService);
                    finishedTest = true;
                    session.setTraceSimpleHandler(handler);
                    session.setPreExecuteEnd(TraceResult.SqlTraceType.MULTI_NODE_QUERY);
                    if (!session.isKilled()) {
                        handler.execute();
                    } else {
                        DDLTraceManager.getInstance().endDDL(shardingService, "Query was interrupted");
                        session.handleSpecial(oriRrs, false, null);
                        ErrorPacket errPacket = new ErrorPacket();
                        errPacket.setPacketId(session.getShardingService().nextPacketId());
                        errPacket.setErrNo(ErrorCode.ER_QUERY_INTERRUPTED);
                        errPacket.setMessage(StringUtil.encode("Query was interrupted", shardingService.getCharset().getResults()));
                        handleRollbackPacket(errPacket.toBytes(), "Query was interrupted");
                    }
                } catch (Exception e) {
                    DDLTraceManager.getInstance().endDDL(shardingService, "take Connection error:" + e.getMessage());
                    LOGGER.warn(String.valueOf(shardingService) + oriRrs, e);
                    session.handleSpecial(oriRrs, false, null);
                    shardingService.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, AbstractService service) {
    }

    @Override
    public boolean rowResponse(final byte[] row, RowDataPacket rowPacketNull, boolean isLeft, AbstractService service) {
        /* It is impossible arriving here, because we set limit to 0 */
        return false;
    }

    @Override
    public void clearResources() {
        if (closedConnSet != null) {
            closedConnSet.clear();
        }
    }

    @Override
    public void clearAfterFailExecute() {
    }

    @Override
    public void writeRemainBuffer() {

    }

    private void handleRollbackPacket(byte[] data, String reason) {
        ShardingService source = session.getShardingService();
        boolean inTransaction = !source.isAutocommit() || source.isTxStart();
        if (!inTransaction) {
            // normal query
            session.closeAndClearResources(reason);
        } else {
            // Explicit distributed transaction
            source.setTxInterrupt(reason);
        }
        session.getSource().write(data);
    }


    public boolean clearIfSessionClosed() {
        if (session.closed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("session closed without execution,clear resources " + session);
            }
            session.clearResources(true);
            if (releaseDDLLock.compareAndSet(false, true)) {
                session.handleSpecial(oriRrs, false, null);
            }
            this.clearResources();
            return true;
        } else {
            return false;
        }
    }
}
