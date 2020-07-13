/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.net.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * mergeHandler will merge data,if contains aggregate function,use group by handler
 *
 * @author ActionTech
 */
public abstract class MultiNodeMergeHandler extends OwnThreadDMLHandler {

    protected final ReentrantLock lock;
    final List<BaseSelectHandler> exeHandlers;
    protected RouteResultsetNode[] route;
    int reachedConCount = 0;

    public MultiNodeMergeHandler(long id, RouteResultsetNode[] route, boolean autocommit, Session session) {
        super(id, session);
        this.exeHandlers = new ArrayList<>();
        this.lock = new ReentrantLock();
        if (route.length == 0)
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "can not execute empty rrss!");
        for (RouteResultsetNode rrss : route) {
            BaseSelectHandler exeHandler = new BaseSelectHandler(id, rrss, autocommit, session);
            exeHandler.setNextHandler(this);
            this.exeHandlers.add(exeHandler);
        }
        this.route = route;
        session.setRouteResultToTrace(route);
    }

    public MultiNodeMergeHandler(long id, Session session) {
        super(id, session);
        this.lock = new ReentrantLock();
        this.exeHandlers = new ArrayList<>();
    }

    public abstract void execute() throws Exception;

    public List<BaseSelectHandler> getExeHandlers() {
        return exeHandlers;
    }

    protected void recycleConn() {
        synchronized (exeHandlers) {
            for (BaseSelectHandler exeHandler : exeHandlers) {
                terminatePreHandler(exeHandler);
            }
        }
    }

    /**
     * terminatePreHandler
     *
     * @param handler handler
     */
    private void terminatePreHandler(DMLResponseHandler handler) {
        DMLResponseHandler current = handler;
        while (current != null) {
            if (current == this)
                break;
            current.terminate();
            current = current.getNextHandler();
        }
    }

}
