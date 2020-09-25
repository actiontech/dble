package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.stage;

import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.List;

public class CommitStage implements TransactionStage {

    private final NonBlockingSession session;
    private final List<BackendConnection> conns;
    private ImplicitCommitHandler handler;

    public CommitStage(NonBlockingSession session, List<BackendConnection> conns, ImplicitCommitHandler handler) {
        this.session = session;
        this.conns = conns;
        this.handler = handler;
    }

    @Override
    public void onEnterStage() {
        for (BackendConnection con : conns) {
            con.getBackendService().commit();
        }
        session.setDiscard(true);
    }

    @Override
    public TransactionStage next(boolean isFail, String errMsg, MySQLPacket sendData) {
        // clear all resources
        session.clearResources(false);
        if (session.closed()) {
            return null;
        }

        if (isFail) {
            session.setFinishedCommitTime();
            session.setResponseTime(false);
            if (sendData != null) {
                sendData.write(session.getSource());
            } else {
                session.getShardingService().writeErrMessage(ErrorCode.ER_YES, "Unexpected error when commit fail:with no message detail");
            }
        } else if (handler != null) {
            // continue to execute sql
            handler.next();
        } else {
            session.setFinishedCommitTime();
            session.setResponseTime(true);
            if (sendData != null) {
                session.getShardingService().write(sendData);
            } else {
                session.getShardingService().writeOkPacket();
            }
        }
        session.clearSavepoint();
        return null;
    }
}
