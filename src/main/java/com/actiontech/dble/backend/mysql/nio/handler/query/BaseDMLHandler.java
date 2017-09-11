/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.query;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.server.NonBlockingSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BaseDMLHandler implements DMLResponseHandler {
    private static Logger logger = Logger.getLogger(BaseDMLHandler.class);
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
    protected NonBlockingSession session;
    protected AtomicBoolean terminate = new AtomicBoolean(false);
    protected Set<DMLResponseHandler> merges;

    public BaseDMLHandler(long id, NonBlockingSession session) {
        this.id = id;
        this.session = session;
        this.merges = Collections.newSetFromMap(new ConcurrentHashMap<DMLResponseHandler, Boolean>());
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
    public void setLeft(boolean left) {
        this.isLeft = left;
    }

    @Override
    public final Set<DMLResponseHandler> getMerges() {
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
                logger.warn("handler terminate exception:", e);
            }
        }
    }

    protected abstract void onTerminate() throws Exception;

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        // TODO Auto-generated method stub

    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        nextHandler.errorResponse(err, conn);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
    }

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
        // TODO Auto-generated method stub

    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeQueueAvailable() {
        // TODO Auto-generated method stub

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        // TODO Auto-generated method stub

    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        // TODO Auto-generated method stub

    }
}
