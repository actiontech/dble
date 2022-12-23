/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.nio.handler.ddl.ImplicitlyCommitCallback;
import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Lock Tables Handler
 *
 * @author songdabin
 */
public class LockTablesHandler extends DefaultMultiNodeHandler implements ExecutableHandler {

    private final RouteResultset rrs;
    private final boolean autocommit;
    private final ImplicitlyCommitCallback implicitlyCommitCallback;

    public LockTablesHandler(NonBlockingSession session, RouteResultset rrs, ImplicitlyCommitCallback implicitlyCommitCallback) {
        super(session);
        if (rrs.getNodes() == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        this.rrs = rrs;
        unResponseRrns.addAll(Arrays.asList(rrs.getNodes()));
        this.autocommit = session.getShardingService().isAutocommit();
        this.implicitlyCommitCallback = implicitlyCommitCallback;
    }

    public void execute() throws Exception {
        session.getShardingService().setLockTable(true);
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
        session.getShardingService().setLockTable(false);
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
    public void handleOkResponse(byte[] data, @NotNull AbstractService service) {
        if (clearIfSessionClosed(session)) {
            return;
        }
        final RouteResultsetNode node = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
        if (node.getSqlType() == ServerParse.UNLOCK) {
            session.releaseConnection((BackendConnection) service.getConnection());
        }
    }

    @Override
    protected void finish(byte[] ok) {
        if (this.isFail()) {
            if (!session.closed()) {
                if (session.getShardingService().isAutocommit()) {
                    session.closeAndClearResources(error);
                }
                handleEndPacket(createErrPkg(this.error, 0));
            }
            return;
        }
        OkPacket okPacket = new OkPacket();
        okPacket.read(ok);
        okPacket.setPacketId(session.getShardingService().nextPacketId());
        okPacket.setServerStatus(session.getShardingService().isAutocommit() ? 2 : 1);
        session.multiStatementPacket(okPacket);
        handleEndPacket(okPacket);
    }

    private void handleEndPacket(MySQLPacket packet) {
        if (implicitlyCommitCallback != null)
            implicitlyCommitCallback.callback();
        TxnLogHelper.putTxnLog(session.getShardingService(), this.rrs);
        session.clearResources(false);
        packet.write(session.getSource());
    }
}
