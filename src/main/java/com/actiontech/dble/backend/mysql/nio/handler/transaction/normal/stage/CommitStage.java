package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.List;

public class CommitStage implements TransactionStage {

    private final NonBlockingSession session;
    private final List<MySQLConnection> conns;
    private ImplicitCommitHandler handler;

    public CommitStage(NonBlockingSession session, List<MySQLConnection> conns, ImplicitCommitHandler handler) {
        this.session = session;
        this.conns = conns;
        this.handler = handler;
    }

    @Override
    public void onEnterStage() {
        for (MySQLConnection con : conns) {
            con.commit();
        }
        session.setDiscard(true);
    }

    @Override
    public TransactionStage next(boolean isFail, String errMsg, byte[] sendData) {
        // clear all resources
        session.clearResources(false);
        if (session.closed()) {
            return null;
        }
        session.getTransactionManager().getNormalTransactionHandler().clearResources();
        if (isFail) {
            session.setFinishedCommitTime();
            session.setResponseTime(false);
            session.getSource().write(sendData);
        } else if (handler != null) {
            // continue to execute sql
            handler.next();
        } else {
            if (sendData != null) {
                session.getPacketId().set(sendData[3]);
            } else {
                sendData = session.getOkByteArray();
            }
            session.setFinishedCommitTime();
            session.setResponseTime(true);
            boolean multiStatementFlag = session.getIsMultiStatement().get();
            session.getSource().write(sendData);
            session.multiStatementNextSql(multiStatementFlag);
        }
        session.clearSavepoint();
        return null;
    }
}
