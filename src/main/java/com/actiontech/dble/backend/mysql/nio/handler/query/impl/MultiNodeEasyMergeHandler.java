/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * mergeHandler will merge data,if contains aggregate function,use group by handler
 *
 * @author ActionTech
 */
public class MultiNodeEasyMergeHandler extends MultiNodeMergeHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeEasyMergeHandler.class);
    private int rowEndConCount = 0;
    private Set<String> globalBackNodes;

    public MultiNodeEasyMergeHandler(long id, RouteResultsetNode[] route, boolean autocommit, NonBlockingSession session, Set<String> globalBackNodes) {
        super(id, route, autocommit, session, true);
        this.merges.add(this);
        this.globalBackNodes = globalBackNodes;
    }

    @Override
    public void execute() {
        synchronized (exeHandlers) {
            if (terminate.get())
                return;

            if (Thread.currentThread().getName().contains("complexQueryExecutor")) {
                doExecute();
            } else {
                DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                    @Override
                    public void run() {
                        doExecute();
                    }
                });
            }
        }
    }

    private void doExecute() {
        for (BaseDMLHandler exeHandler : exeHandlers) {
            if (exeHandler instanceof BaseSelectHandler) {
                BaseSelectHandler baseSelectHandler = (BaseSelectHandler) exeHandler;
                session.setHandlerStart(baseSelectHandler); //base start execute
                try {
                    BackendConnection exeConn = baseSelectHandler.initConnection();
                    exeConn.getBackendService().setComplexQuery(true);
                    baseSelectHandler.execute(exeConn.getBackendService());
                } catch (Exception e) {
                    baseSelectHandler.connectionError(e, baseSelectHandler.getRrss());
                    return;
                }
            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof,
                                 boolean isLeft, @NotNull AbstractService service) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(service.toString() + "'s field is reached.");
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
                nextHandler.fieldEofResponse(null, null, fieldPackets, null, this.isLeft, service);
            }
            startEasyMerge();
            if (++reachedConCount == route.length) {
                session.trace(t -> t.allBackendConnReceive());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, @NotNull AbstractService service) {
        if (terminate.get())
            return true;
        return nextHandler.rowResponse(null, rowPacket, this.isLeft, service);
    }

    @Override
    public void rowEofResponse(byte[] data, boolean isLeft, @NotNull AbstractService service) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(service.toString() + " 's rowEof is reached.");
        }

        if (this.terminate.get())
            return;
        lock.lock();
        try {
            if (++rowEndConCount == route.length) {
                session.setHandlerEnd(this);
                nextHandler.rowEofResponse(null, this.isLeft, service);
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

    @Override
    public HandlerType type() {
        return HandlerType.EASY_MERGE;
    }

    public Set<String> getGlobalBackNodes() {
        return globalBackNodes;
    }

}
