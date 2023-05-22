package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.stage;

import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.List;

public class RollbackStage implements TransactionStage {

    private NonBlockingSession session;
    private final List<MySQLConnection> conns;

    public RollbackStage(NonBlockingSession session, List<MySQLConnection> conns) {
        this.session = session;
        this.conns = conns;
    }

    @Override
    public void onEnterStage() {
        for (final MySQLConnection conn : conns) {
            conn.rollback();
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
        session.setResponseTime(false);
        if (isFail) {
            session.getSource().write(sendData);
            return null;
        }

        if (sendData != null) {
            session.getPacketId().set(sendData[3]);
        } else {
            sendData = session.getOkByteArray();
        }
        boolean multiStatementFlag = session.getIsMultiStatement().get();
        session.getSource().write(sendData);
        session.multiStatementNextSql(multiStatementFlag);
        session.clearSavepoint();
        return null;
    }
}
