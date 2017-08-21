/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend.mysql.nio.handler;

import io.mycat.backend.BackendConnection;
import io.mycat.config.ErrorCode;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.server.NonBlockingSession;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mycat
 */
public abstract class MultiNodeHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(MultiNodeHandler.class);
    protected final ReentrantLock lock = new ReentrantLock();
    protected final NonBlockingSession session;
    private AtomicBoolean isFailed = new AtomicBoolean(false);
    protected volatile String error;
    protected byte packetId;
    protected final AtomicBoolean errorRepsponsed = new AtomicBoolean(false);

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


    protected boolean canClose(BackendConnection conn, boolean tryErrorFinish) {
        // realse this connection if safe
        session.releaseConnectionIfSafe(conn, false);
        boolean allFinished = false;
        if (tryErrorFinish) {
            allFinished = this.decrementCountBy(1);
            this.tryErrorFinished(allFinished);
        }

        return allFinished;
    }

    public void connectionError(Throwable e, BackendConnection conn) {
        this.setFail("backend connect: " + e);
        LOGGER.warn("backend connect", e);
        this.tryErrorFinished(decrementCountBy(1));
    }

    public void errorResponse(byte[] data, BackendConnection conn) {
        session.releaseConnectionIfSafe(conn, false);
        ErrorPacket err = new ErrorPacket();
        err.read(data);
        String errmsg = new String(err.message);
        this.setFail(errmsg);
        LOGGER.warn("error response from " + conn + " err " + errmsg + " code:" + err.errno);
        this.tryErrorFinished(this.decrementCountBy(1));
    }

    public boolean clearIfSessionClosed(NonBlockingSession session) {
        if (session.closed()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("session closed ,clear resources " + session);
            }

            session.clearResources(true);
            this.clearResources();
            return true;
        } else {
            return false;
        }

    }

    protected boolean decrementCountBy(int finished) {
        boolean zeroReached = false;
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
        packetId = 0;
    }

    protected ErrorPacket createErrPkg(String errmgs) {
        ErrorPacket err = new ErrorPacket();
        lock.lock();
        try {
            err.packetId = ++packetId;
        } finally {
            lock.unlock();
        }
        err.errno = ErrorCode.ER_UNKNOWN_ERROR;
        err.message = StringUtil.encode(errmgs, session.getSource().getCharset());
        return err;
    }

    protected void tryErrorFinished(boolean allEnd) {
        if (allEnd && !session.closed()) {
            if (errorRepsponsed.compareAndSet(false, true)) {
                createErrPkg(this.error).write(session.getSource());
            }
            // clear session resources,release all
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("error all end ,clear session resource ");
            }
            if (session.getSource().isAutocommit()) {
                session.closeAndClearResources(error);
            } else {
                session.getSource().setTxInterrupt(this.error);
                // clear resouces
                clearResources();
            }
        }
    }

    public void connectionClose(BackendConnection conn, String reason) {
        this.setFail("closed connection:" + reason + " con:" + conn);
        boolean finished = false;
        lock.lock();
        try {
            finished = (this.nodeCount == 0);

        } finally {
            lock.unlock();
        }
        if (finished == false) {
            finished = this.decrementCountBy(1);
        }
        if (error == null) {
            error = "back connection closed ";
        }
        tryErrorFinished(finished);
    }

    public void clearResources() {
    }

    @Override
    public void relayPacketResponse(byte[] relayPacket, BackendConnection conn) {
    }

    @Override
    public void endPacketResponse(byte[] endPacket, BackendConnection conn) {
    }
}