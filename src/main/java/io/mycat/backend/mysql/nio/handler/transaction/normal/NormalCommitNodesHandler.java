package io.mycat.backend.mysql.nio.handler.transaction.normal;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.transaction.AbstractCommitNodesHandler;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.server.NonBlockingSession;
import io.mycat.util.StringUtil;

public class NormalCommitNodesHandler extends AbstractCommitNodesHandler {
    protected byte[] sendData;

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
        conn.quit();
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        LOGGER.warn("backend connect", e);
        String errmsg = new String(StringUtil.encode(e.getMessage(), session.getSource().getCharset()));
        this.setFail(errmsg);
        conn.quit();
        if (decrementCountBy(1)) {
            cleanAndFeedback();
        }
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
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
