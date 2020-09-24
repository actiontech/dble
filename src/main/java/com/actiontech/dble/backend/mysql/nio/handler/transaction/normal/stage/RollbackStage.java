package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal.stage;

import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionStage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.NonBlockingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RollbackStage implements TransactionStage {
    private static final Logger LOGGER = LoggerFactory.getLogger(RollbackStage.class);
    private NonBlockingSession session;
    private final List<BackendConnection> conns;

    public RollbackStage(NonBlockingSession session, List<BackendConnection> conns) {
        this.session = session;
        this.conns = conns;
    }

    @Override
    public void onEnterStage() {
        for (final BackendConnection conn : conns) {
            conn.getBackendService().rollback();
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

        session.setResponseTime(false);

        LOGGER.info("GET INTO THE NET LEVEL AND THE RESULT IS " + isFail);
        if (isFail) {
            if (sendData != null) {
                sendData.write(session.getSource());
            } else {
                session.getShardingService().writeErrMessage(ErrorCode.ER_YES, "Unexpected error when rollback fail:with no message detail");
            }
            return null;
        }


        if (sendData != null) {
            sendData.write(session.getSource());
        } else {
            session.getShardingService().writeOkPacket();
        }
        session.clearSavepoint();
        return null;
    }
}
