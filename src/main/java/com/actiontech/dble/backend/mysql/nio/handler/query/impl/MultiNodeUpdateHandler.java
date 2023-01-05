/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;

public class MultiNodeUpdateHandler extends MultiNodeMergeHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeUpdateHandler.class);
    private int rowEndConCount = 0;
    private long affectedRows;

    public MultiNodeUpdateHandler(long id, Session session, RouteResultsetNode[] route, boolean autocommit) {
        super(id, route, autocommit, session, false);
        this.merges.add(this);
    }


    public void execute() {
        synchronized (exeHandlers) {
            if (terminate.get())
                return;

            if (Thread.currentThread().getName().contains("complexQueryExecutor")) {
                doExecute();
            } else {
                DbleServer.getInstance().getComplexQueryExecutor().execute(() -> doExecute());
            }
        }
    }

    private void doExecute() {
        for (BaseDMLHandler exeHandler : exeHandlers) {
            if (exeHandler instanceof BaseUpdateHandler) {
                BaseUpdateHandler baseUpdateHandler = (BaseUpdateHandler) exeHandler;
                session.setHandlerStart(baseUpdateHandler); //base start execute
                try {
                    BackendConnection exeConn = baseUpdateHandler.initConnection();
                    exeConn.getBackendService().setComplexQuery(true);
                    baseUpdateHandler.execute(exeConn.getBackendService());
                } catch (Exception e) {
                    baseUpdateHandler.connectionError(e, baseUpdateHandler.getRrss());
                    return;
                }
            }
        }
    }

    @Override
    public void okResponse(byte[] ok, @NotNull AbstractService service) {
        if (this.terminate.get())
            return;
        startEasyMerge();
        boolean executeResponse = ((MySQLResponseService) service).syncAndExecute();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received ok response ,executeResponse:" + executeResponse + " from " + service);
        }

        if (executeResponse) {
            OkPacket okPacket = new OkPacket();
            okPacket.read(ok);
            lock.lock();
            try {
                affectedRows += okPacket.getAffectedRows();
                ((MySQLResponseService) service).backendSpecialCleanUp();
                if (++rowEndConCount != route.length) {
                    return;
                }
                okPacket.setAffectedRows(affectedRows);
                nextHandler.okResponse(okPacket.toBytes(), service);
            } finally {
                lock.unlock();
            }
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
        return HandlerType.EASY_MERGE_UPDATE;
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPackets, byte[] eof, boolean isLeft, @Nonnull AbstractService service) {

    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, @Nonnull AbstractService service) {
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, @Nonnull AbstractService service) {

    }
}
