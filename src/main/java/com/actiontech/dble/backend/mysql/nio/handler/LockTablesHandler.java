/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Lock Tables Handler
 *
 * @author songdabin
 */
public class LockTablesHandler extends MultiNodeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockTablesHandler.class);

    private final RouteResultset rrs;
    private final boolean autocommit;

    public LockTablesHandler(NonBlockingSession session, RouteResultset rrs) {
        super(session);
        this.rrs = rrs;
        unResponseRrns.addAll(Arrays.asList(rrs.getNodes()));
        this.autocommit = session.getShardingService().isAutocommit();
    }

    public void execute() throws Exception {
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

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session)) {
            return;
        }
        conn.getBackendService().setResponseHandler(this);
        conn.getBackendService().setSession(session);
        conn.getBackendService().execute(node, session.getShardingService(), autocommit);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getBackendService().getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
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
    public void okResponse(byte[] data, AbstractService service) {
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
                if (this.isFail() || session.closed()) {
                    tryErrorFinished(true);
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
                ok.write(session.getSource());
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, AbstractService service) {
        LOGGER.info("unexpected packet for " +
                service + " bound by " + session.getSource() +
                ": field's eof");
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        LOGGER.info("unexpected packet for " +
                service + " bound by " + session.getSource() +
                ": row data packet");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        LOGGER.info("unexpected packet for " +
                service + " bound by " + session.getSource() +
                ": row's eof");
    }

}
