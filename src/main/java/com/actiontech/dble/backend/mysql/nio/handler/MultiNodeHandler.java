/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public abstract class MultiNodeHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeHandler.class);
    protected final ReentrantLock lock = new ReentrantLock();
    protected final NonBlockingSession session;
    protected final AtomicBoolean errorResponse = new AtomicBoolean(false);
    protected AtomicBoolean isFailed = new AtomicBoolean(false);
    protected volatile String error;
    protected Set<RouteResultsetNode> unResponseRrns = new HashSet<>();
    protected int errorConnsCnt = 0;
    protected boolean firstResponsed = false;

    public MultiNodeHandler(NonBlockingSession session) {
        if (session == null) {
            throw new IllegalArgumentException("session is null!");
        }
        this.session = session;
    }

    public boolean isFail() {
        return isFailed.get();
    }

    public void setFail(String errMsg) {
        if (isFailed.compareAndSet(false, true)) {
            error = errMsg;
        } else {
            error = error + "\n" + errMsg;
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        this.setFail("backend connect: " + e);
        LOGGER.info("backend connect", e);
        boolean finished;
        lock.lock();
        try {
            errorConnsCnt++;
            finished = canResponse();
        } finally {
            lock.unlock();
        }
        this.tryErrorFinished(finished);
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

    protected boolean[] decrementToZeroAndCheckNode(MySQLResponseService service) {
        boolean zeroReached = false;
        boolean justRemoved;
        lock.lock();
        try {
            RouteResultsetNode rNode = (RouteResultsetNode) service.getAttachment();
            justRemoved = unResponseRrns.remove(rNode);
            if (justRemoved) {
                zeroReached = canResponse();
            }
        } finally {
            lock.unlock();
        }
        return new boolean[]{zeroReached, justRemoved};
    }

    protected boolean decrementToZero(MySQLResponseService service) {
        boolean zeroReached;
        lock.lock();
        try {
            RouteResultsetNode rNode = (RouteResultsetNode) service.getAttachment();
            unResponseRrns.remove(rNode);
            zeroReached = canResponse();
        } finally {
            lock.unlock();
        }
        return zeroReached;
    }

    protected void reset() {
        errorConnsCnt = 0;
        firstResponsed = false;
        unResponseRrns.clear();
        isFailed.set(false);
        error = null;
    }

    protected ErrorPacket createErrPkg(String errMsg, int errorCode) {
        ErrorPacket err = new ErrorPacket();
        lock.lock();
        try {
            err.setPacketId(session.getShardingService().nextPacketId());
        } finally {
            lock.unlock();
        }
        err.setErrNo(errorCode == 0 ? ErrorCode.ER_UNKNOWN_ERROR : errorCode);
        err.setMessage(StringUtil.encode(errMsg, session.getShardingService().getCharset().getResults()));
        return err;
    }

    protected boolean canResponse() {
        if (firstResponsed) {
            return false;
        }
        if (unResponseRrns.size() == errorConnsCnt) {
            firstResponsed = true;
            return true;
        }
        return false;
    }

    void tryErrorFinished(boolean allEnd) {
        if (allEnd && !session.closed()) {
            // clear session resources,release all
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("error all end ,clear session resource ");
            }
            clearSessionResources();
            if (errorResponse.compareAndSet(false, true)) {
                createErrPkg(this.error, 0).write(session.getSource());
            }
        }
    }

    private void clearSessionResources() {
        if (session.getShardingService().isAutocommit()) {
            session.closeAndClearResources(error);
        } else {
            session.getShardingService().setTxInterrupt(this.error);
            this.clearResources();
        }
    }

    public void connectionClose(AbstractService service, String reason) {
        this.setFail("closed connection:" + reason + " con:" + service.toString());
        if (error == null) {
            error = "back connection closed ";
        }
        RouteResultsetNode rNode = (RouteResultsetNode) ((MySQLResponseService) service).getAttachment();
        session.getTargetMap().remove(rNode);
        ((MySQLResponseService) service).setResponseHandler(null);
        tryErrorFinished(decrementToZero((MySQLResponseService) service));
    }

    public void clearResources() {
    }
}
