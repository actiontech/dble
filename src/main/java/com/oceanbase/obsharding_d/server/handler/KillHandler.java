/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.server.SessionStage;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

/**
 * @author mycat
 */
public final class KillHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KillHandler.class);

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
            service.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id: " + id);
            return;
        } else if (!(killConn.getService() instanceof ShardingService) || !((ShardingService) killConn.getService()).getUser().equals(service.getUser())) {
            service.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "can't kill other user's connection: " + id);
            return;
        }

        NonBlockingSession killSession = ((ShardingService) killConn.getService()).getSession2();
        if (killSession.getTransactionManager().getXAStage() != null ||
                killSession.getSessionStage() == SessionStage.Init || killSession.getSessionStage() == SessionStage.Finished) {
            service.writeOkPacket();
            return;
        }

        killSession.setKilled(true);
        // return ok to front connection that sends kill query
        service.writeOkPacket();

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
            service.writeOkPacket();
            return;
        }

        //todo kill should be rewrite
        FrontendConnection fc = findFrontConn(id);
        if (fc == null) {
            service.writeErrMessage(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
            return;
        }
        if (!Objects.equals(fc.getFrontEndService().getUser(), service.getUser())) {
            service.writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "can't kill other user's connection" + id);
            return;
        }
        LOGGER.info("{} {}", fc, "killed by self");
        fc.getFrontEndService().killAndClose("kill by self");

        service.writeOkPacket();
    }

    private static FrontendConnection findFrontConn(long connId) {
        FrontendConnection fc = null;
        IOProcessor[] processors = OBsharding_DServer.getInstance().getFrontProcessors();
        for (IOProcessor p : processors) {
            if ((fc = p.getFrontends().get(connId)) != null) {
                break;
            }
        }
        return fc;
    }

}
