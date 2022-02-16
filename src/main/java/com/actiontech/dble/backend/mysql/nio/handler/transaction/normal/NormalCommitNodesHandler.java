/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AbstractCommitNodesHandler;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

public class NormalCommitNodesHandler extends AbstractCommitNodesHandler {
    protected byte[] sendData;

    @Override
    public void commit() {
        lock.lock();
        try {
            reset();
        } finally {
            lock.unlock();
        }
        int position = 0;
        unResponseRrns.addAll(session.getTargetKeys());
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
        if (decrementToZero(conn)) {
            if (sendData == null) {
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
        if (decrementToZero(conn)) {
            cleanAndFeedback();
        }
    }

    // should be not happen
    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.info("backend connect", e);
        this.setFail(e.getMessage());
        boolean finished;
        lock.lock();
        try {
            errorConnsCnt++;
            finished = canResponse();
        } finally {
            lock.unlock();
        }
        if (finished) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionClose(final BackendConnection conn, final String reason) {
        if (checkClosedConn(conn)) {
            return;
        }
        this.setFail(reason);
        RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
        session.getTargetMap().remove(rNode);
        conn.setResponseHandler(null);
        if (decrementToZero(conn)) {
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
        final boolean isFail = isFailed.get();
        if (isFail) {
            isFailed.set(false);
            final String errorMsg = this.error;
            this.error = null;
            LOGGER.warn("front connection[{}] commit error, because that [{}]", session.getSource(), errorMsg);
            createErrPkg(errorMsg).write(session.getSource());
            setResponseTime(false);
        } else {
            boolean multiStatementFlag = session.getIsMultiStatement().get();
            setResponseTime(true);
            session.getSource().write(send);
            session.multiStatementNextSql(multiStatementFlag);
        }
    }

    protected void setResponseTime(boolean isSuccess) {
    }
}
