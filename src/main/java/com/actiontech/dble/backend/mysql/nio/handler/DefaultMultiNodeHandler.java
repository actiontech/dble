package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.apache.commons.lang.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;

public abstract class DefaultMultiNodeHandler extends MultiNodeHandler {

    protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultMultiNodeHandler.class);

    public DefaultMultiNodeHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public final void connectionError(Throwable e, Object attachment) {
        LOGGER.warn("backend connect", e);
        this.setFail("backend connect: " + e.getMessage());
        boolean zeroReached;
        lock.lock();
        try {
            errorConnsCnt++;
            zeroReached = canResponse();
        } finally {
            lock.unlock();
        }
        if (zeroReached) {
            finish(null);
        }
    }

    @Override
    public final void errorResponse(byte[] err, @Nonnull AbstractService service) {
        MySQLResponseService responseService = (MySQLResponseService) service;
        boolean executeResponse = responseService.syncAndExecute();
        if (!executeResponse) {
            responseService.getConnection().businessClose("unfinished sync");
            RouteResultsetNode rNode = (RouteResultsetNode) responseService.getAttachment();
            session.getTargetMap().remove(rNode);
        }

        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        this.setFail(errMsg);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("receive error [{}] from {}", errMsg, responseService);
        }
        handleErrorResponse(errPacket, responseService);
        if (decrementToZero(responseService)) {
            finish(null);
        }
    }

    public void handleErrorResponse(ErrorPacket err, @NotNull AbstractService service) {
    }

    @Override
    public void okResponse(byte[] ok, @Nonnull AbstractService service) {
        MySQLResponseService responseService = (MySQLResponseService) service;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("receive ok from {}", responseService);
        }
        boolean executeResponse = responseService.syncAndExecute();
        if (executeResponse) {
            handleOkResponse(ok, service);
            if (decrementToZero(responseService)) {
                finish(ok);
            }
        }
    }

    protected void handleOkResponse(byte[] ok, @Nonnull AbstractService service) {
    }

    @Override
    public final void connectionClose(@NotNull AbstractService service, String reason) {
        MySQLResponseService responseService = (MySQLResponseService) service;
        String closeReason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getSchema() + "],threadID[" +
                responseService.getConnection().getThreadId() + "]} was closed ,reason is [" + reason + "]";
        LOGGER.warn(closeReason);
        boolean removed;
        boolean zeroReached;
        RouteResultsetNode rNode = (RouteResultsetNode) responseService.getAttachment();
        lock.lock();
        try {
            removed = unResponseRrns.remove(rNode);
            zeroReached = canResponse();
            if (removed) {
                this.setFail(closeReason);
                handleConnectionClose(responseService, rNode, reason);
            }
        } finally {
            lock.unlock();
        }

        if (removed && zeroReached) {
            finish(null);
        }
    }

    protected void handleConnectionClose(MySQLResponseService service, RouteResultsetNode rNode, String reason) {
        session.getTargetMap().remove(rNode);
        service.setResponseHandler(null);
    }

    protected abstract void finish(byte[] ok);

    @Override
    public void connectionAcquired(BackendConnection connection) {
        throw new NotImplementedException();
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {
        throw new NotImplementedException();
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        throw new NotImplementedException();
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @NotNull AbstractService service) {
        throw new NotImplementedException();
    }

}
