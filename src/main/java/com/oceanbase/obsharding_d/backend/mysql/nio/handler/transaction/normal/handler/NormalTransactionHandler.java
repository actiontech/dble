/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.normal.handler;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.DefaultMultiNodeHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.StageRecorder;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionCallback;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionHandler;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionStage;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.normal.stage.CommitStage;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.normal.stage.RollbackStage;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.mysql.ErrorPacket;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class NormalTransactionHandler extends DefaultMultiNodeHandler implements TransactionHandler {

    private volatile TransactionStage currentStage;
    private volatile MySQLPacket sendData;

    public NormalTransactionHandler(NonBlockingSession session) {
        super(session);
    }

    @Override
    public void commit() {
        commit(null);
    }

    @Override
    public void commit(TransactionCallback transactionCallback) {
        if (session.getTargetCount() <= 0) {
            CommitStage commitStage = new CommitStage(session, null, transactionCallback);
            commitStage.next(false, null, null);
            return;
        }

        reset();
        unResponseRrns.addAll(session.getTargetKeys());
        List<BackendConnection> conns = new ArrayList<>(session.getTargetCount());
        BackendConnection conn;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            conn = session.getTarget(rrn);
            conn.getBackendService().setResponseHandler(this);
            conns.add(conn);
        }
        changeStageTo(new CommitStage(session, conns, transactionCallback));
    }

    @Override
    public void syncImplicitCommit() throws SQLException {
        StageRecorder stageRecorder = new StageRecorder();
        if (session.getTargetCount() <= 0) {
            CommitStage commitStage = new CommitStage(session, null, stageRecorder);
            commitStage.next(false, null, null);
            stageRecorder.check();
            return;
        }

        reset();
        unResponseRrns.addAll(session.getTargetKeys());
        List<BackendConnection> conns = new ArrayList<>(session.getTargetCount());
        BackendConnection conn;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            conn = session.getTarget(rrn);
            conn.getBackendService().setResponseHandler(this);
            conns.add(conn);
        }
        changeStageTo(new CommitStage(session, conns, stageRecorder));
        stageRecorder.check();
    }

    @Override
    public void rollback() {
        rollback(null);
    }

    @Override
    public void rollback(TransactionCallback transactionCallback) {
        RollbackStage rollbackStage;
        if (session.getTargetCount() <= 0) {
            rollbackStage = new RollbackStage(session, null, transactionCallback);
            rollbackStage.next(false, null, sendData);
            return;
        }

        reset();
        List<BackendConnection> conns = new ArrayList<>(session.getTargetCount());
        BackendConnection conn;
        for (RouteResultsetNode node : session.getTargetKeys()) {
            conn = session.getTarget(node);
            if (!conn.isClosed()) {
                unResponseRrns.add(node);
                conn.getBackendService().setResponseHandler(this);
                conns.add(conn);
            }
        }

        if (conns.isEmpty()) {
            rollbackStage = new RollbackStage(session, null, transactionCallback);
            rollbackStage.next(false, null, null);
        } else {
            rollbackStage = new RollbackStage(session, conns, transactionCallback);
            changeStageTo(rollbackStage);
        }
    }

    @Override
    public void turnOnAutoCommit(MySQLPacket previousSendData) {
        this.sendData = previousSendData;
    }

    private void changeStageTo(TransactionStage newStage) {
        if (newStage != null) {
            this.currentStage = newStage;
            this.currentStage.onEnterStage();
        }
    }

    private TransactionStage next() {
        MySQLPacket data = null;
        if (isFail()) {
            data = createErrPkg(error, 0);
        } else if (sendData != null) {
            data = sendData;
        }
        return this.currentStage.next(isFail(), null, data);
    }

    @Override
    public void handleErrorResponse(ErrorPacket err, @NotNull AbstractService service) {
        service.getConnection().businessClose("rollback/commit return error response.");
    }

    @Override
    protected void finish(byte[] ok) {
        changeStageTo(next());
    }

    @Override
    public void clearResources() {
        this.currentStage = null;
        this.sendData = null;
    }

}
