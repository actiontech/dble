/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.SessionStage;
import com.actiontech.dble.util.StringUtil;

import java.util.Map;
import java.util.concurrent.locks.LockSupport;

/**
 * @author mycat
 */
public final class KillHandler {
    private KillHandler() {
    }

    public enum Type {
        KILL_QUERY, KILL_CONNECTION
    }

    public static void handle(Type type, String id, ServerConnection c) {
        if (StringUtil.isEmpty(id)) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "NULL connection id");
            return;
        }
        // parse id
        long idLong;
        try {
            idLong = Long.parseLong(id);
        } catch (NumberFormatException e) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Invalid connection id:" + id);
            return;
        }

        if (type == Type.KILL_CONNECTION) {
            killConnection(idLong, c);
        } else {
            killQuery(idLong, c);
        }
    }

    /**
     * kill query
     *
     * @param id connection id
     * @param c  serverConnection
     */
    private static void killQuery(long id, ServerConnection c) {
        FrontendConnection killConn;
        if (id == c.getId()) {
            c.writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "Query was interrupted.");
            return;
        }

        killConn = findFrontConn(id);
        if (killConn == null) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
            return;
        } else if (!killConn.getUser().equals(c.getUser())) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "can't kill other user's connection" + id);
            return;
        }

        NonBlockingSession killSession = ((ServerConnection) killConn).getSession2();
        if (killSession.getTransactionManager().getXAStage() != null ||
                killSession.getSessionStage() == SessionStage.Init || killSession.getSessionStage() == SessionStage.Finished) {
            boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
            getOkPacket(c).write(c);
            c.getSession2().multiStatementNextSql(multiStatementFlag);
            return;
        }

        killSession.setKilled(true);
        // return ok to front connection that sends kill query
        boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
        getOkPacket(c).write(c);
        c.getSession2().multiStatementNextSql(multiStatementFlag);

        while (true) {
            if (!killSession.isKilled()) {
                break;
            }
            if (killSession.isDiscard()) {
                break;
            }
            LockSupport.parkNanos(10000);
        }

        if (killSession.isKilled() && killSession.isDiscard()) {
            // discard backend conn in session target map
            Map<RouteResultsetNode, BackendConnection> target = killSession.getTargetMap();
            MySQLConnection conn;
            for (BackendConnection backendConnection : target.values()) {
                conn = (MySQLConnection) backendConnection;
                if (conn.isExecuting()) {
                    conn.execCmd("kill query " + conn.getThreadId());
                    conn.close("Query was interrupted.");
                }
            }
        }
    }

    /**
     * kill connection
     *
     * @param id connection id
     * @param c  serverConnection
     */
    private static void killConnection(long id, ServerConnection c) {
        // kill myself
        if (id == c.getId()) {
            OkPacket packet = getOkPacket(c);
            packet.setPacketId(0);
            packet.write(c);
            return;
        }

        FrontendConnection fc = findFrontConn(id);
        if (fc == null) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
            return;
        } else if (!fc.getUser().equals(c.getUser())) {
            c.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "can't kill other user's connection" + id);
            return;
        }
        fc.killAndClose("killed");

        boolean multiStatementFlag = c.getSession2().getIsMultiStatement().get();
        getOkPacket(c).write(c);
        c.getSession2().multiStatementNextSql(multiStatementFlag);
    }

    private static FrontendConnection findFrontConn(long connId) {
        FrontendConnection fc = null;
        NIOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
        for (NIOProcessor p : processors) {
            if ((fc = p.getFrontends().get(connId)) != null) {
                break;
            }
        }
        return fc;
    }

    private static OkPacket getOkPacket(ServerConnection c) {
        byte packetId = (byte) c.getSession2().getPacketId().get();
        OkPacket packet = new OkPacket();
        packet.setPacketId(++packetId);
        packet.setAffectedRows(0);
        packet.setServerStatus(2);
        c.getSession2().multiStatementPacket(packet, packetId);
        return packet;
    }

}
