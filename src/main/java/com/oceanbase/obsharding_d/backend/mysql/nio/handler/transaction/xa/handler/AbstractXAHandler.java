/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.handler;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.DefaultMultiNodeHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionCallback;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.stage.XAStage;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public abstract class AbstractXAHandler extends DefaultMultiNodeHandler {

    private static Logger logger = LoggerFactory.getLogger(AbstractXAHandler.class);
    protected volatile XAStage currentStage;
    protected volatile boolean interruptTx = true;
    protected volatile MySQLPacket packetIfSuccess;
    protected volatile TransactionCallback transactionCallback;

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

    @Override
    public void handleOkResponse(byte[] ok, @NotNull AbstractService service) {
        this.currentStage.onConnectionOk((MySQLResponseService) service);
    }

    @Override
    public void handleErrorResponse(ErrorPacket err, @NotNull AbstractService service) {
        currentStage.onConnectionError((MySQLResponseService) service, err.getErrNo());
    }

    @Override
    public void handleConnectionClose(MySQLResponseService service, RouteResultsetNode rNode, final String reason) {
        currentStage.onConnectionClose(service);
    }

    @Override
    protected void finish(byte[] ok) {
        changeStageTo(next());
    }

    @Override
    public void clearResources() {
        this.currentStage = null;
        this.interruptTx = true;
        this.packetIfSuccess = null;
        this.transactionCallback = null;
    }

    public Set<RouteResultsetNode> setUnResponseRrns() {
        Set<RouteResultsetNode> targetKeys = session.getTargetKeys();
        lock.lock();
        try {
            this.unResponseRrns.addAll(targetKeys);
            return targetKeys;
        } finally {
            lock.unlock();
        }
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

    public TransactionCallback getTransactionCallback() {
        return transactionCallback;
    }

}
