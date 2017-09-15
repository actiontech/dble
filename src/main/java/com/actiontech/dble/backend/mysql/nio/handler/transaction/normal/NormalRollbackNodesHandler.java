/*
* Copyright (C) 2016-2017 ActionTech.
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
        final int initCount = session.getTargetCount();
        lock.lock();
        try {
            reset(initCount);
        } finally {
            lock.unlock();
        }
        int position = 0;
        for (final RouteResultsetNode node : session.getTargetKeys()) {
            final BackendConnection conn = session.getTarget(node);
            if (conn.isClosed()) {
                lock.lock();
                try {
                    nodeCount--;
                } finally {
                    lock.unlock();
                }
                continue;
            }
            position++;
            conn.setResponseHandler(this);
            conn.rollback();
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
        if (decrementCountBy(1)) {
            if (sendData == null) {
                sendData = ok;
            }
            cleanAndFeedback();
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.read(err);
        String errmsg = new String(errPacket.getMessage());
        this.setFail(errmsg);
        conn.quit(); //quit to rollback
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("backend connect", e);
        String errmsg = new String(StringUtil.encode(e.getMessage(), session.getSource().getCharset().getResults()));
        this.setFail(errmsg);
        conn.quit(); //quit if not rollback
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        // quitted
        this.setFail(reason);
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
