/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AbstractCommitNodesHandler;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.server.NonBlockingSession;

public class NormalCommitNodesHandler extends AbstractCommitNodesHandler {
    protected byte[] sendData;

    @Override
    public boolean init() {
        if (initResponse()) {
            return true;
        } else {
            String reason = "backend conn closed";
            session.clearResources(true);
            createErrPkg(reason).write(session.getSource());
            return false;
        }
    }

    @Override
    public void clearResources() {
        sendData = null;
    }

    public NormalCommitNodesHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    protected boolean executeCommit(MySQLConnection mysqlCon, int position) {
        mysqlCon.commit();
        return true;
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        this.waitUntilSendFinish();
        if (decrementCountBy(1)) {
            if (sendData == null) {
                sendData = ok;
            }
            cleanAndFeedback();
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        this.waitUntilSendFinish();
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        this.setFail(errMsg);
        conn.quit();
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        this.waitUntilSendFinish();
        LOGGER.warn("backend connect", e);
        this.setFail(e.getMessage());
        conn.quit();
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        this.waitUntilSendFinish();
        this.setFail(reason);
        conn.quit();
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    private void cleanAndFeedback() {
        byte[] send = sendData;
        // clear all resources
        session.clearResources(false);
        if (session.closed()) {
            return;
        }
        if (this.isFail()) {
            createErrPkg(error).write(session.getSource());
        } else {
            session.getSource().write(send);
        }
    }
}
