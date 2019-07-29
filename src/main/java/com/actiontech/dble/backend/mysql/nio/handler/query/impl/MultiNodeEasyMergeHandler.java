/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * mergeHandler will merge data,if contains aggregate function,use group by handler
 *
 * @author ActionTech
 */
public class MultiNodeEasyMergeHandler extends MultiNodeMergeHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeEasyMergeHandler.class);
    private int rowEndConCount = 0;

    public MultiNodeEasyMergeHandler(long id, RouteResultsetNode[] route, boolean autocommit, NonBlockingSession session) {
        super(id, route, autocommit, session);
        this.merges.add(this);
    }

    @Override
    public void execute() throws Exception {
        synchronized (exeHandlers) {
            if (terminate.get())
                return;
            for (BaseSelectHandler exeHandler : exeHandlers) {
                session.setHandlerStart(exeHandler); //base start execute
                MySQLConnection exeConn = exeHandler.initConnection();
                if (exeConn != null) {
                    exeConn.setComplexQuery(true);
                    exeHandler.execute(exeConn);
                }
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(conn.toString() + "'s field is reached.");
        }
        session.setHandlerStart(this);
        // if terminated
        if (terminate.get()) {
            return;
        }
        lock.lock(); // for combine
        try {
            if (this.fieldPackets.isEmpty()) {
                this.fieldPackets = fieldPackets;
                nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, conn);
            }
            startEasyMerge();
            if (++reachedConCount == route.length) {
                session.allBackendConnReceive();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        if (terminate.get())
            return true;
        return nextHandler.rowResponse(null, rowPacket, this.isLeft, conn);
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, BackendConnection conn) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(conn.toString() + " 's rowEof is reached.");
        }

        if (this.terminate.get())
            return;
        lock.lock();
        try {
            if (++rowEndConCount == route.length) {
                session.setHandlerEnd(this);
                nextHandler.rowEofResponse(null, this.isLeft, conn);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void ownThreadJob(Object... objects) {
    }

    @Override
    protected void terminateThread() throws Exception {
        recycleConn();
    }

    @Override
    protected void recycleResources() {
    }
}
