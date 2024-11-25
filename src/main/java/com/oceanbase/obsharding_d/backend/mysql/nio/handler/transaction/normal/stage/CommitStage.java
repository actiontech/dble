/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.normal.stage;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionCallback;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.StageRecorder;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionStage;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.VariationSQLException;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.server.NonBlockingSession;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class CommitStage extends Stage implements TransactionStage {

    private final NonBlockingSession session;
    private final List<BackendConnection> conns;
    private TransactionCallback transactionCallback;

    public CommitStage(NonBlockingSession session, List<BackendConnection> conns, TransactionCallback transactionCallback) {
        this.session = session;
        this.conns = conns;
        this.transactionCallback = transactionCallback;
    }

    public CommitStage(NonBlockingSession session, List<BackendConnection> conns, StageRecorder stageRecorder) {
        this.session = session;
        this.conns = conns;
        this.isSync = true;
        this.stageRecorder = stageRecorder;
        this.lock = new ReentrantLock();
        this.done = lock.newCondition();
    }

    @Override
    public void onEnterStage() {
        for (BackendConnection con : conns) {
            con.getBackendService().commit();
        }
        if (isSync) {
            waitFinished();
        }
        session.setDiscard(true);
    }

    @Override
    public TransactionStage next(boolean isFail, String errMsg, MySQLPacket sendData) {
        if (session.closed()) {
            return null;
        }
        if (isSync) {
            syncNext(isFail, errMsg, sendData);
        } else {
            // clear all resources
            session.clearResources(false);
            asyncNext(isFail, errMsg, sendData);
        }
        session.clearSavepoint();
        return null;
    }

    private void asyncNext(boolean isFail, String errMsg, MySQLPacket sendData) {
        if (session.closed())
            return;

        session.setFinishedCommitTime();
        if (transactionCallback != null)
            transactionCallback.callback();

        session.getTransactionManager().getNormalTransactionHandler().clearResources();
        if (isFail) {
            if (sendData != null) {
                sendData.write(session.getSource());
            } else {
                session.getShardingService().writeErrMessage(ErrorCode.ER_YES, "Unexpected error when commit fail:with no message detail");
            }
        } else {
            if (sendData != null) {
                session.getShardingService().write(sendData);
            } else {
                session.getShardingService().writeOkPacket();
            }
        }
    }

    private void syncNext(boolean isFail, String errMsg, MySQLPacket sendData) {
        session.getTransactionManager().getNormalTransactionHandler().clearResources();
        if (isFail) {
            session.getShardingService().setPacketId(0);
            if (sendData != null) {
                stageRecorder.setException(new VariationSQLException(sendData));
            } else {
                stageRecorder.setException(new SQLException("Unexpected error when commit fail:with no message detail", "HY000", ErrorCode.ER_YES));
            }
        }
        signalFinished();
    }
}
