/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.stage.CommitStage;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.stage.RollbackStage;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class NormalTransactionHandler extends MultiNodeHandler implements TransactionHandler {

    private static Logger logger = LoggerFactory.getLogger(NormalTransactionHandler.class);

    private volatile TransactionStage currentStage;
    private volatile byte[] sendData;

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
        List<MySQLConnection> conns = new ArrayList<>(session.getTargetCount());
        BackendConnection conn;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            conn = session.getTarget(rrn);
            conn.setResponseHandler(this);
            conns.add((MySQLConnection) conn);
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
        List<MySQLConnection> conns = new ArrayList<>(session.getTargetCount());
        BackendConnection conn;
        for (RouteResultsetNode node : session.getTargetKeys()) {
            conn = session.getTarget(node);
            if (!conn.isClosed()) {
                unResponseRrns.add(node);
                conn.setResponseHandler(this);
                conns.add((MySQLConnection) conn);
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
    public void turnOnAutoCommit(byte[] previousSendData) {
        this.sendData = previousSendData;
        this.packetId = previousSendData[3];
        this.packetId--;
    }

    private void changeStageTo(TransactionStage newStage) {
        if (newStage != null) {
            this.currentStage = newStage;
            this.currentStage.onEnterStage();
        }
    }

    private TransactionStage next() {
        byte[] data = null;
        if (isFail()) {
            data = createErrPkg(error).toBytes();
        } else if (sendData != null) {
            data = sendData;
        }
        return this.currentStage.next(isFail(), null, data);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (logger.isDebugEnabled()) {
            logger.debug("receive ok from " + conn);
        }
        conn.syncAndExecute();
        if (decrementToZero(conn)) {
            changeStageTo(next());
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        conn.syncAndExecute();
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        this.setFail(errMsg);

        MySQLConnection mysqlCon = (MySQLConnection) conn;
        if (logger.isDebugEnabled()) {
            logger.debug("receive error [" + errMsg + "] from " + mysqlCon);
        }

        mysqlCon.closeWithoutRsp("rollback/commit return error response.");
        if (decrementToZero(mysqlCon)) {
            changeStageTo(next());
        }
    }

    @Override
    public void connectionClose(final BackendConnection conn, final String reason) {
        boolean[] result = decrementToZeroAndCheckNode(conn);
        boolean finished = result[0];
        boolean justRemoved = result[1];
        if (justRemoved) {
            String closeReason = "Connection {dbInstance[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "],threadID[" +
                    ((MySQLConnection) conn).getThreadId() + "]} was closed ,reason is [" + reason + "]";
            this.setFail(closeReason);

            RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
            session.getTargetMap().remove(rNode);
            conn.setResponseHandler(null);
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
        packetId = 0;
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
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, BackendConnection conn) {
        logger.warn("unexpected filed eof response in normal transaction");
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        logger.warn("unexpected row response in normal transaction");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        logger.warn("unexpected row eof response in normal transaction");
    }

}
