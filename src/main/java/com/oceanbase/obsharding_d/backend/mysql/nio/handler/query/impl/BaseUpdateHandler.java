/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.impl;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.CharsetUtil;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.mysql.FieldPacket;
import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class BaseUpdateHandler extends BaseDMLHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseUpdateHandler.class);

    private final boolean autocommit;
    private final RouteResultsetNode rrss;
    private final NonBlockingSession serverSession;

    public BaseUpdateHandler(long id, RouteResultsetNode rrss, boolean autocommit, Session session) {
        super(id, session, rrss);
        serverSession = (NonBlockingSession) session;
        this.rrss = rrss;
        this.autocommit = autocommit;
    }

    public BackendConnection initConnection() throws Exception {
        if (serverSession.closed()) {
            return null;
        }

        BackendConnection exeConn = serverSession.getTarget(rrss);
        if (serverSession.tryExistsCon(exeConn, rrss)) {
            exeConn.getBackendService().setRowDataFlowing(true);
            exeConn.getBackendService().setResponseHandler(this);
            return exeConn;
        } else {
            ShardingNode dn = OBsharding_DServer.getInstance().getConfig().getShardingNodes().get(rrss.getName());
            //autocommit is serverSession.getWriteSource().isAutocommit() && !serverSession.getWriteSource().isTxStart()
            final BackendConnection newConn = dn.getConnection(dn.getDatabase(), autocommit, rrss);
            serverSession.bindConnection(rrss, newConn);
            newConn.getBackendService().setResponseHandler(this);
            newConn.getBackendService().setRowDataFlowing(true);
            return newConn;
        }
    }

    public void execute(MySQLResponseService service) {
        TraceManager.crossThread(service, "base-sql-execute", serverSession.getShardingService());
        if (serverSession.closed()) {
            service.setRowDataFlowing(false);
            serverSession.clearResources(true);
            return;
        }
        service.setSession(serverSession);
        if (service.getConnection().isClosed()) {
            service.setRowDataFlowing(false);
            serverSession.onQueryError("failed or cancelled by other thread".getBytes());
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(service.toString() + " send sql:" + rrss.getStatement());
        }
        service.executeMultiNode(rrss, serverSession.getShardingService(), autocommit);
    }

    public RouteResultsetNode getRrss() {
        return rrss;
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        LOGGER.debug("receive ok packet for sync context, service {}", service);
        if (terminate.get()) {
            return;
        }
        nextHandler.okResponse(ok, service);
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService conn) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, @NotNull AbstractService service) {
    }

    /**
     * 1. if some connection's thread status is await. 2. if some connection's
     * thread status is running.
     */
    @Override
    public void connectionError(Throwable e, Object attachment) {
        if (terminate.get())
            return;
        String errMsg;
        if (e instanceof MySQLOutPutException) {
            errMsg = e.getMessage() == null ? e.toString() : e.getMessage();
        } else if (e instanceof NullPointerException) {
            errMsg = e.getMessage() == null ? e.toString() : e.getMessage();
        } else {
            RouteResultsetNode node = (RouteResultsetNode) attachment;
            errMsg = "can't connect to shardingNode[" + node.getName() + "],due to " + e.getMessage();
        }
        LOGGER.warn(errMsg, e);
        serverSession.onQueryError(errMsg.getBytes());
    }

    @Override
    public void connectionClose(@NotNull AbstractService service, String reason) {
        if (terminate.get())
            return;
        LOGGER.warn(service.toString() + "|connectionClose()|" + reason);
        reason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getConnection().getSchema() + "],threadID[" +
                ((BackendConnection) service.getConnection()).getThreadId() + "]} was closed ,reason is [" + reason + "]";
        serverSession.onQueryError(reason.getBytes());
    }

    @Override
    public void errorResponse(byte[] err, @NotNull AbstractService service) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg;
        try {
            errMsg = new String(errPacket.getMessage(), CharsetUtil.getJavaCharset(service.getCharset().getResults()));
        } catch (UnsupportedEncodingException e) {
            errMsg = "UnsupportedEncodingException:" + service.getCharset();
        }
        LOGGER.info(service.toString() + errMsg);
        if (terminate.get())
            return;
        serverSession.onQueryError(errMsg.getBytes());
    }

    @Override
    protected void onTerminate() {
        if (autocommit && !serverSession.getShardingService().isLockTable()) {
            this.serverSession.releaseConnection(rrss, false);
        } else {
            //the connection should wait until the connection running finish
            this.serverSession.waitFinishConnection(rrss);
        }
    }

    @Override
    public HandlerType type() {
        return HandlerType.BASE_UPDATE;
    }

}
