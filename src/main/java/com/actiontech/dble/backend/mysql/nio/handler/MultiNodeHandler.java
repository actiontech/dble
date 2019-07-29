/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public abstract class MultiNodeHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeHandler.class);
    protected final ReentrantLock lock = new ReentrantLock();
    protected final NonBlockingSession session;
    private AtomicBoolean isFailed = new AtomicBoolean(false);
    protected volatile String error;
    protected byte packetId;
    protected final AtomicBoolean errorResponse = new AtomicBoolean(false);

    public MultiNodeHandler(NonBlockingSession session) {
        if (session == null) {
            throw new IllegalArgumentException("session is null!");
        }
        this.session = session;
    }

    public void setFail(String errMsg) {
        isFailed.set(true);
        error = errMsg;
    }

    public boolean isFail() {
        return isFailed.get();
    }

    protected int nodeCount;

    public void connectionError(Throwable e, BackendConnection conn) {
        this.setFail("backend connect: " + e);
        LOGGER.info("backend connect", e);
        this.tryErrorFinished(decrementCountBy(1));
    }


    public boolean clearIfSessionClosed(NonBlockingSession nonBlockingSession) {
        if (nonBlockingSession.closed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("session closed ,clear resources " + nonBlockingSession);
            }

            nonBlockingSession.clearResources(true);
            this.clearResources();
            return true;
        } else {
            return false;
        }

    }

    public boolean clearIfSessionClosed(NonBlockingSession nonBlockingSession, boolean needRelease) {
        if (nonBlockingSession.closed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("session closed ,clear resources " + nonBlockingSession);
            }
            if (needRelease) {
                nonBlockingSession.clearResources(true);
            }
            this.clearResources();
            return true;
        } else {
            return false;
        }

    }

    protected boolean decrementCountBy(int finished) {
        boolean zeroReached;
        lock.lock();
        try {
            nodeCount -= finished;
            zeroReached = nodeCount == 0;
        } finally {
            lock.unlock();
        }
        return zeroReached;
    }

    protected void reset(int initCount) {
        nodeCount = initCount;
        isFailed.set(false);
        error = null;
        packetId = (byte) session.getPacketId().get();
    }

    protected ErrorPacket createErrPkg(String errMsg) {
        ErrorPacket err = new ErrorPacket();
        lock.lock();
        try {
            err.setPacketId(++packetId);
        } finally {
            lock.unlock();
        }
        err.setErrNo(ErrorCode.ER_UNKNOWN_ERROR);
        err.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        return err;
    }

    protected void tryErrorFinished(boolean allEnd) {
        if (allEnd && !session.closed()) {
            // clear session resources,release all
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("error all end ,clear session resource ");
            }
            clearSessionResources();
            if (errorResponse.compareAndSet(false, true)) {
                createErrPkg(this.error).write(session.getSource());
            }
        }
    }

    protected void clearSessionResources() {
        if (session.getSource().isAutocommit()) {
            session.closeAndClearResources(error);
        } else {
            session.getSource().setTxInterrupt(this.error);
            this.clearResources();
        }
    }

    public void connectionClose(BackendConnection conn, String reason) {
        this.setFail("closed connection:" + reason + " con:" + conn);
        boolean finished;
        lock.lock();
        try {
            finished = (this.nodeCount == 0);

        } finally {
            lock.unlock();
        }
        if (!finished) {
            finished = this.decrementCountBy(1);
        }
        if (error == null) {
            error = "back connection closed ";
        }
        tryErrorFinished(finished);
    }

    public void clearResources() {
    }
}
