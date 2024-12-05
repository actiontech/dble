/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.route.util.RouteResultCopy;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.server.parser.ServerParse;
import com.oceanbase.obsharding_d.server.trace.TraceResult;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.DDLTraceHelper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MultiNodeDdlPrepareHandler extends BaseDDLHandler {

    private static final String STMT = "select 1";
    private volatile boolean finishedTest = false;
    private final MultiNodeDDLExecuteHandler nextHandler;

    public MultiNodeDdlPrepareHandler(NonBlockingSession session, RouteResultset rrs, @Nullable Object attachment, ImplicitlyCommitCallback implicitlyCommitCallback) {
        super(session, rrs, attachment, implicitlyCommitCallback);
        this.rrs = RouteResultCopy.rrCopy(rrs, ServerParse.DDL, STMT);
        this.nextHandler = new MultiNodeDDLExecuteHandler(session, this.oriRrs, attachment, this.rrs, implicitlyCommitCallback);
        this.stage = DDLTraceHelper.Stage.test_ddl_conn;
        this.traceMessage = "execute-for-ddl-prepare";
    }

    @Override
    protected boolean isMustWrite() {
        return true;
    }

    @Override
    protected void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed()) return;
        DDLTraceHelper.log(session.getShardingService(), d -> d.infoByNode(node.getName(), stage, DDLTraceHelper.Status.get_conn, "Get " + conn.toString()));
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setSession(session);
        conn.getBackendService().setTesting(true);
        conn.getBackendService().setComplexQuery(true);
        conn.getBackendService().execute(node, session.getShardingService(), sessionAutocommit && !session.getShardingService().isTxStart());
    }

    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        if (!((MySQLResponseService) service).syncAndExecute()) {
            LOGGER.debug("MultiNodeDdlPrepareHandler syncAndExecute!");
        } else {
            LOGGER.debug("MultiNodeDdlPrepareHandler syncAndExecute finished!");
        }
    }

    @Override
    public void rowEofResponse(final byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        MySQLResponseService responseService = (MySQLResponseService) service;
        final RouteResultsetNode node = (RouteResultsetNode) responseService.getAttachment();
        final ShardingService shardingService = session.getShardingService();
        DDLTraceHelper.log(session.getShardingService(), d -> d.infoByNode(node.getName(), stage, DDLTraceHelper.Status.succ));
        if (clearIfSessionClosed()) return;

        lock.lock();
        try {
            responseService.setTesting(false);

            if (!decrementToZero(node, STATUS_OK)) return;
            if (isErr()) {
                if (writeToClientFlag.compareAndSet(false, true)) {
                    specialHandling(false, this.getErrMsg());
                    this.err.setPacketId(shardingService.nextPacketId());
                    handleEndPacket(this.err);
                }
            } else {
                finishedTest = true;
                if (writeToClientFlag.compareAndSet(false, true)) {
                    session.setTraceSimpleHandler(nextHandler);
                    session.setPreExecuteEnd(TraceResult.SqlTraceType.MULTI_NODE_QUERY);
                    if (session.isKilled()) {
                        String errInfo = "Query was interrupted";
                        setErrPkg(errInfo, ErrorCode.ER_QUERY_INTERRUPTED);
                        specialHandling(false, this.getErrMsg());
                        this.err.setPacketId(shardingService.nextPacketId());
                        handleEndPacket(this.err);
                        return;
                    }
                    try {
                        DDLTraceHelper.log(session.getShardingService(), d -> d.info(stage, DDLTraceHelper.Status.succ));
                        nextHandler.execute();
                    } catch (Exception e) {
                        String errInfo = "MultiNodeDDLExecuteHandler.execute exception " + e.getMessage();
                        setErrPkg(errInfo, ErrorCode.ERR_HANDLE_DATA);
                        specialHandling(false, this.getErrMsg());
                        this.err.setPacketId(shardingService.nextPacketId());
                        nextHandler.clearAfterFailExecute();
                        handleEndPacket(this.err);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected boolean checkIsAlreadyClosed(final RouteResultsetNode node, final MySQLResponseService mysqlResponseService) {
        lock.lock();
        try {
            if (finishedTest) return true;
            if (closedConnSet.contains(mysqlResponseService)) {
                nodeResponseStatus.put(node, STATUS_CONN_CLOSE);
                return true;
            } else {
                closedConnSet.add(mysqlResponseService);
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean clearIfSessionClosed() {
        if (session.closed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("session closed without execution,clear resources " + session);
            }
            String msg = "Current session closed";
            specialHandling(false, msg);
            DDLTraceHelper.finish(session.getShardingService());
            session.clearResources(true);
            this.clearResources();
            return true;
        } else {
            return false;
        }
    }
}
