/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query.impl;

import com.actiontech.dble.backend.mysql.nio.handler.query.BaseDMLHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.OwnThreadDMLHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.route.RouteResultsetNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * mergeHandler will merge data,if contains aggregate function,use group by handler
 *
 * @author ActionTech
 */
public abstract class MultiNodeMergeHandler extends OwnThreadDMLHandler {

    protected final ReentrantLock lock;
    final List<BaseDMLHandler> exeHandlers;
    protected RouteResultsetNode[] route;
    int reachedConCount = 0;
    private Set<String> dependencies;

    public MultiNodeMergeHandler(long id, RouteResultsetNode[] route, boolean autocommit, Session session, boolean isSelect) {
        super(id, session);
        this.exeHandlers = new ArrayList<>();
        dependencies = new HashSet<>();
        this.lock = new ReentrantLock();
        if (route.length == 0)
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "can not execute empty rrss!");
        for (RouteResultsetNode rrss : route) {
            BaseDMLHandler exeHandler;
            if (isSelect) {
                exeHandler = new BaseSelectHandler(id, rrss, autocommit, session);
                exeHandler.setNextHandler(this);
            } else {
                exeHandler = new BaseUpdateHandler(id, rrss, autocommit, session);
                exeHandler.setNextHandler(this);
            }
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

    public List<BaseDMLHandler> getExeHandlers() {
        return exeHandlers;
    }

    protected void recycleConn() {
        synchronized (exeHandlers) {
            for (BaseDMLHandler exeHandler : exeHandlers) {
                terminatePreHandler(exeHandler);
            }
        }
    }

    public RouteResultsetNode[] getRoute() {
        return route;
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
            session.trace(t -> t.setBackendTerminateByComplex(this));
            current = current.getNextHandler();
        }
    }

    public Set<String> getDependencies() {
        return dependencies;
    }

    public void setDependencies(Set<String> dependencies) {
        this.dependencies = dependencies;
    }

    public int getReachedConCount() {
        return reachedConCount;
    }
}
