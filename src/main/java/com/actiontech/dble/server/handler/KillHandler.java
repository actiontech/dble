/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

import com.actiontech.dble.server.SessionStage;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
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

    public static void handle(Type type, String id, ShardingService service) {
        if (StringUtil.isEmpty(id)) {
            service.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "NULL connection id");
            return;
        }
        // parse id
        long idLong;
        try {
            idLong = Long.parseLong(id);
        } catch (NumberFormatException e) {
            service.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Invalid connection id:" + id);
            return;
        }

        if (type == Type.KILL_CONNECTION) {
            killConnection(idLong, service);
        } else {
            killQuery(idLong, service);
        }
    }

    /**
     * kill query
     *
     * @param id      connection id
     * @param service serverConnection
     */
    private static void killQuery(long id, ShardingService service) {
        FrontendConnection killConn;
        if (id == service.getConnection().getId()) {
            service.writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "Query was interrupted.");
            return;
        }

        killConn = findFrontConn(id);
        if (killConn == null) {
            service.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
            return;
        } else if (!killConn.isManager() && !((ShardingService) killConn.getService()).getUser().equals(service.getUser())) {
            service.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "can't kill other user's connection" + id);
            return;
        }

        NonBlockingSession killSession = ((ShardingService) killConn.getService()).getSession2();
        if (killSession.getTransactionManager().getXAStage() != null ||
                killSession.getSessionStage() == SessionStage.Init || killSession.getSessionStage() == SessionStage.Finished) {
            getOkPacket(service).write(service.getConnection());
            return;
        }

        killSession.setKilled(true);
        // return ok to front connection that sends kill query
        getOkPacket(service).write(service.getConnection());

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
            for (BackendConnection backendConnection : target.values()) {
                if (backendConnection.getBackendService().isExecuting()) {
                    backendConnection.getBackendService().execCmd("kill query " + backendConnection.getThreadId());
                    backendConnection.close("Query was interrupted.");
                }
            }
        }
    }

    /**
     * kill connection
     *
     * @param id      connection id
     * @param service serverConnection
     */
    private static void killConnection(long id, ShardingService service) {
        // kill myself
        if (id == service.getConnection().getId()) {
            OkPacket packet = getOkPacket(service);
            packet.setPacketId(0);
            packet.write(service.getConnection());
            return;
        }

        //todo kill should be rewrite
        /*FrontendConnection fc = findFrontConn(id);
        if (fc == null) {
            service.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
            return;
        } else if (!fc.getUser().equals(service.getUser())) {
            service.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "can't kill other user's connection" + id);
            return;
        }
        fc.killAndClose("killed");*/

        getOkPacket(service).write(service.getConnection());
    }

    private static FrontendConnection findFrontConn(long connId) {
        FrontendConnection fc = null;
        IOProcessor[] processors = DbleServer.getInstance().getFrontProcessors();
        for (IOProcessor p : processors) {
            if ((fc = p.getFrontends().get(connId)) != null) {
                break;
            }
        }
        return fc;
    }

    private static OkPacket getOkPacket(ShardingService service) {
        byte packetId = (byte) service.getSession2().getPacketId().get();
        OkPacket packet = new OkPacket();
        packet.setPacketId(++packetId);
        packet.setAffectedRows(0);
        packet.setServerStatus(2);
        service.getSession2().multiStatementPacket(packet, packetId);
        return packet;
    }

}
