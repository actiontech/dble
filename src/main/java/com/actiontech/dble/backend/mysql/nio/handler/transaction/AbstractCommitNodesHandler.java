/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeHandler;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractCommitNodesHandler extends MultiNodeHandler implements CommitNodesHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractCommitNodesHandler.class);
    protected Set<BackendConnection> closedConnSet;
    public AbstractCommitNodesHandler(NonBlockingSession session) {
        super(session);
    }

    protected abstract boolean executeCommit(MySQLConnection mysqlCon, int position);



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
