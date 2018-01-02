/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCommitNodesHandler extends MultiNodeHandler implements CommitNodesHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractCommitNodesHandler.class);
    private Lock lockForErrorHandle = new ReentrantLock();
    private Condition sendFinished = lockForErrorHandle.newCondition();
    private volatile boolean sendFinishedFlag = false;
    protected Set<BackendConnection> closedConnSet;
    public AbstractCommitNodesHandler(NonBlockingSession session) {
        super(session);
    }

    protected abstract boolean executeCommit(MySQLConnection mysqlCon, int position);

    @Override
    public void commit() {
        final int initCount = session.getTargetCount();
        lock.lock();
        try {
            reset(initCount);
        } finally {
            lock.unlock();
        }
        int position = 0;
        //get session's lock before sending commit(in fact, after ended)
        //then the XA transaction will be not killed, if killed ,then we will not commit
        if (session.getXaState() != null && session.getXaState() == TxState.TX_ENDED_STATE) {
            if (!session.cancelableStatusSet(NonBlockingSession.CANCEL_STATUS_COMMITTING)) {
                return;
            }
        }

        try {
            sendFinishedFlag = false;
            for (RouteResultsetNode rrn : session.getTargetKeys()) {
                final BackendConnection conn = session.getTarget(rrn);
                conn.setResponseHandler(this);
                if (!executeCommit((MySQLConnection) conn, position++)) {
                    break;
                }
            }
        } finally {
            lockForErrorHandle.lock();
            try {
                sendFinishedFlag = true;
                sendFinished.signalAll();
            } finally {
                lockForErrorHandle.unlock();
            }
        }

    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        LOGGER.info("unexpected packet for " + conn +
                " bound by " + session.getSource() +
                ": field's eof");
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        LOGGER.info("unexpected invocation: connectionAcquired from commit");
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        LOGGER.info("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": field's eof");
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        LOGGER.info("unexpected packet for " +
                conn + " bound by " + session.getSource() +
                ": field's eof");
        return false;
    }

    @Override
    public void writeQueueAvailable() {

    }

    @Override
    public void reset(int initCount) {
        nodeCount = initCount;
        packetId = 0;
    }

    public void debugCommitDelay() {

    }

    protected void waitUntilSendFinish() {
        this.lockForErrorHandle.lock();
        try {
            if (!this.sendFinishedFlag) {
                this.sendFinished.await();
            }
        } catch (Exception e) {
            LOGGER.info("back Response is closed by thread interrupted");
        } finally {
            lockForErrorHandle.unlock();
        }
    }

    protected boolean checkClosedConn(BackendConnection conn) {
        lock.lock();
        try {
            if (closedConnSet == null) {
                closedConnSet = new HashSet<>(1);
                closedConnSet.add(conn);
            } else {
                if (closedConnSet.contains(conn)) {
                    return true;
                }
                closedConnSet.add(conn);
            }
            return false;
        } finally {
            lock.unlock();
        }
    }
}
