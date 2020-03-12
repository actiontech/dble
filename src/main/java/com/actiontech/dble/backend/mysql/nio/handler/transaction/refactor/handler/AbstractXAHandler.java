/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.XATransactionContext;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.refactor.stage.XAStage;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractXAHandler extends MultiNodeHandler {

    private static Logger logger = LoggerFactory.getLogger(AbstractXAHandler.class);
    volatile XAStage currentStage;
    protected XATransactionContext context;

    public AbstractXAHandler(NonBlockingSession session) {
        super(session);
        this.context = new XATransactionContext(session, this);
    }

    protected XAStage next() {
        XAStage next = currentStage.next(isFail());
        if (next == null) {
            if (isFail()) {
                session.getSource().setTxInterrupt(error);
                session.getSource().write(makeErrorPacket(error));
            } else {
                session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_INIT);
                context.clearInvolvedRrns();
                session.clearResources(false);
                if (session.closed()) {
                    return null;
                }
                //            setResponseTime(isSuccess);
                //            byte[] send = sendData;
                removeQuitConn();
                session.getSource().write(OkPacket.OK);
            }
        }
        return next;
    }

    void changeStageTo(XAStage newStage) {
        if (newStage != null) {
            this.reset();
            this.currentStage = newStage;
            this.currentStage.onEnterStage();
        }
    }

    public void setUnResponseRrns() {
        unResponseRrns.addAll(session.getTargetKeys());
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
    public void okResponse(byte[] ok, BackendConnection conn) {
        if (logger.isDebugEnabled()) {
            logger.debug("receive ok from " + conn);
        }
        conn.syncAndExecute();
        this.currentStage.onConnectionOk((MySQLConnection) conn);
        if (decrementToZero(conn)) {
            changeStageTo(next());
        }
    }

    public void fakedResponse(BackendConnection conn) {
        logger.warn("receive faked response");
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
        currentStage.onConnectionError(mysqlCon, errPacket.getErrNo());
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
            String closeReason = "Connection {DataHost[" + conn.getHost() + ":" + conn.getPort() + "],Schema[" + conn.getSchema() + "],threadID[" +
                    ((MySQLConnection) conn).getThreadId() + "]} was closed ,reason is [" + reason + "]";
            this.setFail(closeReason);
            currentStage.onConnectionClose((MySQLConnection) conn);
            if (finished) {
                changeStageTo(next());
            }
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        logger.warn("connection Error in savePointHandler, err:", e);
        boolean finished;
        lock.lock();
        try {
            errorConnsCnt++;
            finished = canResponse();
        } finally {
            lock.unlock();
        }

        currentStage.onConnectError((MySQLConnection) conn);
        if (finished) {
            changeStageTo(next());
        }
    }

    @Override
    public void clearResources() {

    }

    private byte[] makeErrorPacket(String errMsg) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(1);
        errPacket.setErrNo(ErrorCode.ER_UNKNOWN_ERROR);
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        return errPacket.toBytes();
    }

    void removeQuitConn() {
        for (final RouteResultsetNode node : session.getTargetKeys()) {
            final MySQLConnection mysqlCon = (MySQLConnection) session.getTarget(node);
            if (mysqlCon.getXaStatus() != TxState.TX_CONN_QUIT && mysqlCon.getXaStatus() != TxState.TX_ROLLBACKED_STATE) {
                session.getTargetMap().remove(node);
            }
        }
    }

    @Override
    public void writeQueueAvailable() {
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        logger.warn("unexpected connection acquired in xa transaction");
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, BackendConnection conn) {
        logger.warn("unexpected filed eof response in xa transaction");
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        logger.warn("unexpected row response in xa transaction");
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        logger.warn("unexpected row eof response in xa transaction");
    }
}
