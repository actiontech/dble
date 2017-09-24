/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock Tables Handler
 *
 * @author songdabin
 */
public class LockTablesHandler extends MultiNodeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockTablesHandler.class);

    private final RouteResultset rrs;
    private final ReentrantLock lock;
    private final boolean autocommit;

    public LockTablesHandler(NonBlockingSession session, RouteResultset rrs) {
        super(session);
        this.rrs = rrs;
        this.autocommit = session.getSource().isAutocommit();
        this.lock = new ReentrantLock();
    }

    public void execute() throws Exception {
        super.reset(this.rrs.getNodes().length);
        ServerConfig conf = DbleServer.getInstance().getConfig();
        for (final RouteResultsetNode node : rrs.getNodes()) {
            BackendConnection conn = session.getTarget(node);
            if (session.tryExistsCon(conn, node)) {
                innerExecute(conn, node);
            } else {
                // create new connection
                PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
                dn.getConnection(dn.getDatabase(), autocommit, node, this, node);
            }
        }
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        if (clearIfSessionClosed(session)) {
            return;
        }
        conn.setResponseHandler(this);
        conn.execute(node, session.getSource(), autocommit);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }

    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
        boolean executeResponse = conn.syncAndExcute();
        if (executeResponse) {
            if (clearIfSessionClosed(session)) {
                return;
            }
            boolean isEndPack = decrementCountBy(1);
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
                ok.write(session.getSource());
            }
        }
    }

    protected String byte2Str(byte[] data) {
        StringBuilder sb = new StringBuilder();
        for (byte b : data) {
            sb.append(Byte.toString(b));
        }
        return sb.toString();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        LOGGER.error("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": field's eof");
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        LOGGER.warn("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": row data packet");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.error("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": row's eof");
    }

    @Override
    public void writeQueueAvailable() {
        // TODO Auto-generated method stub

    }

}
