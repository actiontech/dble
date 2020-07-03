/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * UnLock Tables Handler
 *
 * @author songdabin
 */
public class UnLockTablesHandler extends MultiNodeHandler implements ResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnLockTablesHandler.class);

    private final boolean autocommit;
    private final String srcStatement;

    public UnLockTablesHandler(NonBlockingSession session, boolean autocommit, String sql) {
        super(session);
        this.autocommit = autocommit;
        this.srcStatement = sql;
    }

    public void execute() {
        Map<RouteResultsetNode, BackendConnection> lockedCons = session.getTargetMap();
        this.reset();
        // if client just send an unlock tables, theres is no lock tables statement, just send back OK
        if (lockedCons.size() == 0) {
            LOGGER.info("find no locked backend connection!" + session.getSource());
            OkPacket ok = new OkPacket();
            ok.setPacketId(session.getShardingService().nextPacketId());
            ok.setPacketLength(7); // the size of unlock table's response OK packet is 7
            ok.setServerStatus(session.getShardingService().isAutocommit() ? 2 : 1);
            ok.write(session.getSource());
            return;
        }
        Map<RouteResultsetNode, BackendConnection> forUnlocks = new HashMap<>(lockedCons.size());
        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : lockedCons.entrySet()) {
            RouteResultsetNode shardingNode = entry.getKey();
            BackendConnection conn = entry.getValue();
            RouteResultsetNode node = new RouteResultsetNode(shardingNode.getName(), ServerParse.UNLOCK, srcStatement);
            forUnlocks.put(node, conn);
            unResponseRrns.add(node);
        }
        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : forUnlocks.entrySet()) {
            RouteResultsetNode node = entry.getKey();
            BackendConnection conn = entry.getValue();
            if (clearIfSessionClosed(session)) {
                return;
            }
            conn.getBackendService().setResponseHandler(this);
            conn.getBackendService().setSession(session);
            try {
                conn.getBackendService().execute(node, session.getShardingService(), autocommit);
            } catch (Exception e) {
                connectionError(e, node);
            }
        }
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        LOGGER.info("unexpected invocation: connectionAcquired from unlock tables");
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            session.releaseConnectionIfSafe(((MySQLResponseService) service), false);
        } else {
            service.getConnection().businessClose("unfinished sync");
            session.getTargetMap().remove(((MySQLResponseService) service).getAttachment());
        }
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        if (!isFail()) {
            setFail(errMsg);
        }
        LOGGER.info("error response from " + service + " err " + errMsg + " code:" + errPacket.getErrNo());

        this.tryErrorFinished(this.decrementToZero(((MySQLResponseService) service)));
    }

    @Override
    public void okResponse(byte[] data, AbstractService service) {
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (executeResponse) {
            boolean isEndPack = decrementToZero(((MySQLResponseService) service));
            session.releaseConnection(((MySQLResponseService) service).getConnection());
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
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
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

    @Override
    public void connectionClose(AbstractService service, String reason) {
        // TODO Auto-generated method stub

    }

}
