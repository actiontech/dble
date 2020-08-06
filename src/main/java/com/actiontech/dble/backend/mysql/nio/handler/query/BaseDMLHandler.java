/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query;

import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BaseDMLHandler implements DMLResponseHandler {
    private static Logger logger = LoggerFactory.getLogger(BaseDMLHandler.class);
    protected final long id;

    /**
     * all pushed down? contains functions
     */
    private boolean allPushDown = false;

    /**
     * field packets list from parent handler
     */
    protected List<FieldPacket> fieldPackets = new ArrayList<>();
    protected BaseDMLHandler nextHandler = null;
    protected boolean isLeft = false;
    protected Session session;
    protected AtomicBoolean terminate = new AtomicBoolean(false);
    protected List<DMLResponseHandler> merges;

    public BaseDMLHandler(long id, Session session) {
        this.id = id;
        this.session = session;
        this.merges = new ArrayList<>();
    }

    @Override
    public final BaseDMLHandler getNextHandler() {
        return this.nextHandler;
    }

    @Override
    public final void setNextHandler(DMLResponseHandler next) {
        this.nextHandler = (BaseDMLHandler) next;
        DMLResponseHandler toAddMergesHandler = next;
        do {
            toAddMergesHandler.getMerges().addAll(this.getMerges());
            toAddMergesHandler = toAddMergesHandler.getNextHandler();
        } while (toAddMergesHandler != null);
    }

    @Override
    public final void setNextHandlerOnly(DMLResponseHandler next) {
        this.nextHandler = (BaseDMLHandler) next;
    }

    @Override
    public void setLeft(boolean left) {
        this.isLeft = left;
    }

    @Override
    public final List<DMLResponseHandler> getMerges() {
        return this.merges;
    }

    public boolean isAllPushDown() {
        return allPushDown;
    }

    public void setAllPushDown(boolean allPushDown) {
        this.allPushDown = allPushDown;
    }

    @Override
    public final void terminate() {
        if (terminate.compareAndSet(false, true)) {
            try {
                onTerminate();
            } catch (Exception e) {
                logger.info("handler terminate exception:", e);
            }
        }
    }

    protected abstract void onTerminate() throws Exception;

    @Override
    public void connectionError(Throwable e, Object attachment) {
        // TODO Auto-generated method stub

    }

    @Override
    public void errorResponse(byte[] err, AbstractService service) {
        nextHandler.errorResponse(err, service);
    }

    @Override
    public void okResponse(byte[] ok, AbstractService service) {
    }

    @Override
    public void connectionClose(AbstractService service, String reason) {
        // TODO Auto-generated method stub

    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        // TODO Auto-generated method stub

    }
}
