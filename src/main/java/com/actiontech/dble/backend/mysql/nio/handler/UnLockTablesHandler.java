/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.parser.ServerParse;
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
            ok.setPacketId(++packetId);
            ok.setPacketLength(7); // the size of unlock table's response OK packet is 7
            ok.setServerStatus(session.getSource().isAutocommit() ? 2 : 1);
            ok.write(session.getSource());
            return;
        }
        Map<RouteResultsetNode, BackendConnection> forUnlocks = new HashMap<>(lockedCons.size());
        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : lockedCons.entrySet()) {
            RouteResultsetNode dataNode = entry.getKey();
            BackendConnection conn = entry.getValue();
            RouteResultsetNode node = new RouteResultsetNode(dataNode.getName(), ServerParse.UNLOCK, srcStatement);
            forUnlocks.put(node, conn);
            unResponseRrns.add(node);
        }
        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : forUnlocks.entrySet()) {
            RouteResultsetNode node = entry.getKey();
            BackendConnection conn = entry.getValue();
            if (clearIfSessionClosed(session)) {
                return;
            }
            conn.setResponseHandler(this);
            conn.setSession(session);
            try {
                conn.execute(node, session.getSource(), autocommit);
            } catch (Exception e) {
                connectionError(e, conn);
            }
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        super.connectionError(e, conn);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        LOGGER.info("unexpected invocation: connectionAcquired from unlock tables");
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExecute();
        if (executeResponse) {
            session.releaseConnectionIfSafe(conn, false);
        } else {
            conn.closeWithoutRsp("unfinished sync");
            session.getTargetMap().remove(conn.getAttachment());
        }
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        if (!isFail()) {
            setFail(errMsg);
        }
        LOGGER.info("error response from " + conn + " err " + errMsg + " code:" + errPacket.getErrNo());

        this.tryErrorFinished(this.decrementToZero(conn));
    }

    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExecute();
        if (executeResponse) {
            boolean isEndPack = decrementToZero(conn);
            session.releaseConnection(conn);
            if (isEndPack) {
                if (this.isFail() || session.closed()) {
                    tryErrorFinished(true);
                    return;
                }
                OkPacket ok = new OkPacket();
                ok.read(data);
                lock.lock();
                try {
                    ok.setPacketId(++packetId);
                    ok.setServerStatus(session.getSource().isAutocommit() ? 2 : 1);
                } finally {
                    lock.unlock();
                }
                session.multiStatementPacket(ok, packetId);
                boolean multiStatementFlag = session.getIsMultiStatement().get();
                ok.write(session.getSource());
                session.multiStatementNextSql(multiStatementFlag);
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        LOGGER.info("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": field's eof");
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        LOGGER.info("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": row data packet");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.info("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": row's eof");
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        // TODO Auto-generated method stub

    }

}
