/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.handler;

import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.stage.CommitStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.stage.RollbackStage;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class NormalTransactionHandler extends MultiNodeHandler implements TransactionHandler {

    private static Logger logger = LoggerFactory.getLogger(NormalTransactionHandler.class);

    private volatile TransactionStage currentStage;
    private volatile MySQLPacket sendData;

    public NormalTransactionHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public void commit() {
        implicitCommit(null);
    }

    @Override
    public void implicitCommit(ImplicitCommitHandler implicitCommitHandler) {
        if (session.getTargetCount() <= 0) {
            CommitStage commitStage = new CommitStage(session, null, implicitCommitHandler);
            commitStage.next(false, null, null);
            return;
        }

        reset();
        unResponseRrns.addAll(session.getTargetKeys());
        List<BackendConnection> conns = new ArrayList<>(session.getTargetCount());
        BackendConnection conn;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            conn = session.getTarget(rrn);
            conn.getBackendService().setResponseHandler(this);
            conns.add(conn);
        }
        changeStageTo(new CommitStage(session, conns, implicitCommitHandler));
    }

    @Override
    public void rollback() {
        RollbackStage rollbackStage;
        if (session.getTargetCount() <= 0) {
            rollbackStage = new RollbackStage(session, null);
            rollbackStage.next(false, null, null);
            return;
        }

        reset();
        List<BackendConnection> conns = new ArrayList<>(session.getTargetCount());
        BackendConnection conn;
        for (RouteResultsetNode node : session.getTargetKeys()) {
            conn = session.getTarget(node);
            if (!conn.isClosed()) {
                unResponseRrns.add(node);
                conn.getBackendService().setResponseHandler(this);
                conns.add(conn);
            }
        }

        if (conns.isEmpty()) {
            rollbackStage = new RollbackStage(session, null);
            rollbackStage.next(false, null, null);
        } else {
            rollbackStage = new RollbackStage(session, conns);
            changeStageTo(rollbackStage);
        }
    }

    @Override
    public void turnOnAutoCommit(MySQLPacket previousSendData) {
        this.sendData = previousSendData;
    }

    private void changeStageTo(TransactionStage newStage) {
        if (newStage != null) {
            this.currentStage = newStage;
            this.currentStage.onEnterStage();
        }
    }

    private TransactionStage next() {
        MySQLPacket data = null;
        if (isFail()) {
            data = createErrPkg(error, 0);
        } else if (sendData != null) {
            data = sendData;
        }
        return this.currentStage.next(isFail(), null, data);
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
        if (logger.isDebugEnabled()) {
            logger.debug("receive ok from " + service);
        }
        ((MySQLResponseService) service).syncAndExecute();
        if (decrementToZero(((MySQLResponseService) service))) {
            changeStageTo(next());
        }
    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        ((MySQLResponseService) service).syncAndExecute();
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        this.setFail(errMsg);

        MySQLResponseService mySQLResponseService = (MySQLResponseService) service;
        if (logger.isDebugEnabled()) {
            logger.debug("receive error [" + errMsg + "] from " + mySQLResponseService);
        }

        mySQLResponseService.getConnection().businessClose("rollback/commit return error response.");
        if (decrementToZero(mySQLResponseService)) {
            changeStageTo(next());
        }
    }

    @Override
    public void connectionClose(final AbstractService service, final String reason) {
        boolean[] result = decrementToZeroAndCheckNode((MySQLResponseService) service);
        boolean finished = result[0];
        boolean justRemoved = result[1];
        if (justRemoved) {
            String closeReason = "Connection {dbInstance[" + service.getConnection().getHost() + ":" + service.getConnection().getPort() + "],Schema[" + ((MySQLResponseService) service).getSchema() + "],threadID[" +
                    ((MySQLResponseService) service).getConnection().getThreadId() + "]} was closed ,reason is [" + reason + "]";
            this.setFail(closeReason);

            RouteResultsetNode rNode = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
            session.getTargetMap().remove(rNode);
            ((MySQLResponseService) service).setResponseHandler(null);
            if (finished) {
                changeStageTo(next());
            }
        }
    }

    // should be not happen
    @Override
    public void connectionError(Throwable e, Object attachment) {
        logger.warn("connection Error in normal transaction handler, err:", e);
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
    public void reset() {
        errorConnsCnt = 0;
        firstResponsed = false;
        unResponseRrns.clear();
        isFailed.set(false);
    }

    @Override
    public void clearResources() {
        this.currentStage = null;
        this.sendData = null;
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        logger.warn("unexpected connection acquired in normal transaction");
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, AbstractService service) {
        logger.warn("unexpected filed eof response in normal transaction");
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        logger.warn("unexpected row response in normal transaction");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, AbstractService service) {
        logger.warn("unexpected row eof response in normal transaction");
    }

}
