/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AbstractCommitNodesHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplictCommitHandler;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

public class NormalCommitNodesHandler extends AbstractCommitNodesHandler {
    protected byte[] sendData;

    @Override
    public void commit() {
        final int initCount = session.getTargetCount();
        if (initCount <= 0) {
            if (implictCommitHandler == null && sendData == null) {
                sendData = session.getOkByteArray();
            }
            cleanAndFeedback();
            return;
        }

        lock.lock();
        try {
            reset(initCount);
        } finally {
            lock.unlock();
        }

        int position = 0;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(rrn);
            conn.setResponseHandler(this);
            if (!executeCommit((MySQLConnection) conn, position++)) {
                break;
            }
        }
    }

    @Override
    public void clearResources() {
        sendData = null;
        implictCommitHandler = null;
        if (closedConnSet != null) {
            closedConnSet.clear();
        }
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
        if (decrementCountBy(1)) {
            if (implictCommitHandler == null && sendData == null) {
                sendData = session.getOkByteArray();
            }
            cleanAndFeedback();
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errMsg = new String(errPacket.getMessage());
        this.setFail(errMsg);
        conn.close("commit response error");
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.info("backend connect", e);
        this.setFail(e.getMessage());
        conn.close("Commit connection Error");
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionClose(final BackendConnection conn, final String reason) {
        if (checkClosedConn(conn)) {
            return;
        }
        this.setFail(reason);
        conn.close("commit connection closed");
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    private void cleanAndFeedback() {
        byte[] send = sendData;
        ImplictCommitHandler handler = implictCommitHandler;
        // clear all resources
        session.clearResources(false);
        if (session.closed()) {
            return;
        }
        if (this.isFail()) {
            createErrPkg(error).write(session.getSource());
            setResponseTime(false);
        } else if (handler != null) {
            // continue to execute sql
            handler.next();
        } else {
            setResponseTime(true);
            session.getSource().write(send);
        }
        session.clearSavepoint();
        session.multiStatementNextSql(session.getIsMultiStatement().get());
    }

    protected void setResponseTime(boolean isSuccess) {
    }

}
