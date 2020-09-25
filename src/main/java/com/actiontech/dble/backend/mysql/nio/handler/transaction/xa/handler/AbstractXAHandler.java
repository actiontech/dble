/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.handler;

import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.stage.XAStage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractXAHandler extends MultiNodeHandler {

    private static Logger logger = LoggerFactory.getLogger(AbstractXAHandler.class);
    protected volatile XAStage currentStage;
    protected volatile boolean interruptTx = true;
    protected volatile MySQLPacket packetIfSuccess;
    protected volatile ImplicitCommitHandler implicitCommitHandler;

    public AbstractXAHandler(NonBlockingSession session) {
        super(session);
    }

    public XAStage next() {
        MySQLPacket sendData = error == null ? null : makeErrorPacket(error);
        return (XAStage) currentStage.next(isFail(), error, sendData);
    }

    protected void changeStageTo(XAStage newStage) {
        if (newStage != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("xa stage will change to {}", newStage.getStage());
            }
            this.reset();
            this.currentStage = newStage;
            this.currentStage.onEnterStage();
        }
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        if (logger.isDebugEnabled()) {
            logger.debug("receive ok from " + service);
        }
        ((MySQLResponseService) service).syncAndExecute();
        this.currentStage.onConnectionOk((MySQLResponseService) service);
        if (decrementToZero((MySQLResponseService) service)) {
            changeStageTo(next());
        }
    }

    public void fakedResponse(MySQLResponseService service, String reason) {
        if (reason != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("receive faked response from " + service + ",because " + reason);
            }
            this.setFail(reason);
        }
        if (decrementToZero(service)) {
            changeStageTo(next());
        }
    }

    public void fakedResponse(RouteResultsetNode rrsn) {
        if (decrementToZero(rrsn)) {
            changeStageTo(next());
        }
    }

    protected boolean decrementToZero(RouteResultsetNode rrsn) {
        boolean zeroReached;
        lock.lock();
        try {
            unResponseRrns.remove(rrsn);
            zeroReached = canResponse();
        } finally {
            lock.unlock();
        }
        return zeroReached;
    }


    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        ((MySQLResponseService) service).syncAndExecute();
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        this.setFail(errMsg);

        if (logger.isDebugEnabled()) {
            logger.debug("receive error [" + errMsg + "] from " + service);
        }
        currentStage.onConnectionError((MySQLResponseService) service, errPacket.getErrNo());
        if (decrementToZero((MySQLResponseService) service)) {
            changeStageTo(next());
        }
    }

    @Override
    public void connectionClose(final AbstractService service, final String reason) {
        boolean[] result = decrementToZeroAndCheckNode((MySQLResponseService) service);
        boolean finished = result[0];
        boolean justRemoved = result[1];
        if (justRemoved) {
            String closeReason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getConnection().getSchema() + "],threadID[" +
                    ((MySQLResponseService) service).getConnection().getThreadId() + "]} was closed ,reason is [" + reason + "]";
            if (logger.isDebugEnabled()) {
                logger.debug(closeReason);
            }
            this.setFail(closeReason);
            currentStage.onConnectionClose((MySQLResponseService) service);
            if (finished) {
                changeStageTo(next());
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("conn[backendId=" + service.getConnection().getId() + "] was closed in gap of two stage");
            }
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        logger.warn("connection Error in xa transaction, err:", e);
        boolean finished;
        lock.lock();
        try {
            errorConnsCnt++;
            finished = canResponse();
        } finally {
            lock.unlock();
        }
        if (finished) {
            changeStageTo(next());
        }
    }

    @Override
    public void clearResources() {
        this.currentStage = null;
        this.interruptTx = true;
        this.packetIfSuccess = null;
        this.implicitCommitHandler = null;
    }

    @Override
    public void reset() {
        errorConnsCnt = 0;
        firstResponsed = false;
        unResponseRrns.clear();
        isFailed.set(false);
    }

    public void setUnResponseRrns() {
        this.unResponseRrns.addAll(session.getTargetKeys());
    }

    public String getXAStage() {
        return currentStage == null ? null : currentStage.getStage();
    }

    public boolean isInterruptTx() {
        return interruptTx;
    }

    public MySQLPacket getPacketIfSuccess() {
        return packetIfSuccess;
    }

    public void setPacketIfSuccess(MySQLPacket packetIfSuccess) {
        this.packetIfSuccess = packetIfSuccess;
    }

    public void interruptTx(String reason) {
        setFail(reason);
        session.getShardingService().setTxInterrupt(reason);
        makeErrorPacket(reason).write(session.getSource());
    }

    private MySQLPacket makeErrorPacket(String errMsg) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setErrNo(ErrorCode.ER_UNKNOWN_ERROR);
        errPacket.setMessage(StringUtil.encode(errMsg, session.getShardingService().getCharset().getResults()));
        return errPacket;
    }

    public ImplicitCommitHandler getImplicitCommitHandler() {
        return implicitCommitHandler;
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        logger.warn("unexpected connection acquired in xa transaction");
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, AbstractService service) {
        logger.warn("unexpected filed eof response in xa transaction");
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        logger.warn("unexpected row response in xa transaction");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        logger.warn("unexpected row eof response in xa transaction");
    }
}
