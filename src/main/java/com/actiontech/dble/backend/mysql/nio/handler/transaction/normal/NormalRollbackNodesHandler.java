/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal;


import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.AbstractRollbackNodesHandler;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.util.StringUtil;

/**
 * @author mycat
 */
public class NormalRollbackNodesHandler extends AbstractRollbackNodesHandler {
    protected byte[] sendData;

    @Override
    public void clearResources() {
        sendData = null;
    }

    public NormalRollbackNodesHandler(NonBlockingSession session) {
        super(session);
    }

    public void rollback() {
        lock.lock();
        try {
            reset();
        } finally {
            lock.unlock();
        }
        int position = 0;
        for (final RouteResultsetNode node : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(node);
            if (!conn.isClosed()) {
                unResponseRrns.add(node);
            }
        }
        for (final RouteResultsetNode node : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(node);
            if (!conn.isClosed()) {
                position++;
                conn.setResponseHandler(this);
                conn.rollback();
            }
        }
        if (position == 0) {
            if (sendData == null) {
                sendData = OkPacket.OK;
            }
            cleanAndFeedback();
        }
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
        conn.close("rollback error response"); //quit to rollback
        if (decrementToZero(conn)) {
            cleanAndFeedback();
        }
    }

    // should be not happen
    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.info("backend connect", e);
        String errMsg = new String(StringUtil.encode(e.getMessage(), session.getSource().getCharset().getResults()));
        this.setFail(errMsg);
        conn.close("rollback connection error"); //quit if not rollback
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
    public void connectionClose(BackendConnection conn, String reason) {
        // quitted
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
        setResponseTime(false);
        if (this.isFail()) {
            createErrPkg(error).write(session.getSource());
        } else {
            boolean multiStatementFlag = session.getIsMultiStatement().get();
            session.getSource().write(send);
            session.multiStatementNextSql(multiStatementFlag);
            session.clearSavepoint();
        }
    }

    protected void setResponseTime(boolean isSuccess) {
    }
}
