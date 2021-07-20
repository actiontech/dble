/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Lock Tables Handler
 *
 * @author songdabin
 */
public class LockTablesHandler extends MultiNodeHandler implements ExecutableHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockTablesHandler.class);

    private final RouteResultset rrs;
    private final boolean autocommit;

    public LockTablesHandler(NonBlockingSession session, RouteResultset rrs) {
        super(session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        this.rrs = rrs;
        unResponseRrns.addAll(Arrays.asList(rrs.getNodes()));
        this.autocommit = session.getShardingService().isAutocommit();
        TxnLogHelper.putTxnLog(session.getShardingService(), rrs);
    }

    public void execute() throws Exception {
        session.getShardingService().setLocked(true);
        session.getTransactionManager().setXaTxEnabled(false, session.getShardingService());
        super.reset();
        for (final RouteResultsetNode node : rrs.getNodes()) {
            BackendConnection conn = session.getTarget(node);
            if (session.tryExistsCon(conn, node)) {
                innerExecute(conn, node);
            } else {
                // create new connection
                ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), session.getShardingService().isTxStart(), autocommit, node, this, node);
            }
        }
    }

    @Override
    public void clearAfterFailExecute() {
        session.getShardingService().setLocked(false);
    }

    @Override
    public void writeRemainBuffer() {

    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session)) {
            return;
        }
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setSession(session);
        conn.getBackendService().execute(node, session.getShardingService(), autocommit && !session.getShardingService().isTxStart());
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getBackendService().getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        super.connectionError(e, attachment);
    }

    @Override
    public void errorResponse(byte[] err, @NotNull AbstractService service) {
        MySQLResponseService responseService = (MySQLResponseService) service;
        boolean executeResponse = responseService.syncAndExecute();
        if (executeResponse) {
            session.releaseConnectionIfSafe(responseService, false);
        } else {
            responseService.getConnection().businessClose("unfinished sync");
            RouteResultsetNode rNode = (RouteResultsetNode) responseService.getAttachment();
            session.getTargetMap().remove(rNode);
        }
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        if (!isFail()) {
            setFail(errMsg);
        }
        LOGGER.info("error response from " + responseService + " err " + errMsg + " code:" + errPacket.getErrNo());
        this.tryErrorFinished(this.decrementToZero(responseService));
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        super.connectionClose(service, reason);
    }

    @Override
    public void okResponse(byte[] data, @NotNull AbstractService service) {
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            if (clearIfSessionClosed(session)) {
                return;
            }
            boolean isEndPack = decrementToZero((MySQLResponseService) service);
            final RouteResultsetNode node = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
            if (node.getSqlType() == ServerParse.UNLOCK) {
                session.releaseConnection((BackendConnection) service.getConnection());
            }
            if (isEndPack) {
                if (this.isFail()) {
                    this.tryErrorFinished(true);
                    return;
                }
                OkPacket ok = new OkPacket();
                ok.read(data);
                lock.lock();
                try {
                    ok.setPacketId(session.getShardingService().nextPacketId());
                    ok.setServerStatus(session.getShardingService().isAutocommit() ? 2 : 1);
                } finally {
                    lock.unlock();
                }
                session.multiStatementPacket(ok);
                handleEndPacket(ok, true);
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {
        LOGGER.info("unexpected packet for " +
                service + " bound by " + session.getSource() +
                ": field's eof");
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        LOGGER.info("unexpected packet for " +
                service + " bound by " + session.getSource() +
                ": row data packet");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        LOGGER.info("unexpected packet for " +
                service + " bound by " + session.getSource() +
                ": row's eof");
    }

    private void handleEndPacket(MySQLPacket packet, boolean isSuccess) {
        session.setResponseTime(isSuccess);
        session.clearResources(false);
        packet.write(session.getSource());
    }

    protected void tryErrorFinished(boolean allEnd) {
        if (allEnd && !session.closed()) {
            LOGGER.warn("Lock tables execution failed");
            // clear session resources,release all
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("error all end ,clear session resource ");
            }
            clearSessionResources();
            if (errorResponse.compareAndSet(false, true)) {
                handleEndPacket(createErrPkg(this.error, 0), false);
            }
        }
    }

    protected void clearSessionResources() {
        if (session.getShardingService().isAutocommit()) {
            session.closeAndClearResources(error);
        } else {
            this.clearResources();
        }
    }
}
