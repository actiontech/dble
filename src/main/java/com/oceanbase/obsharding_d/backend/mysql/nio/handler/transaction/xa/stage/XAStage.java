/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.stage;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.TransactionStage;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.handler.AbstractXAHandler;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.server.NonBlockingSession;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public abstract class XAStage implements TransactionStage {
    protected static final Logger LOGGER = LoggerFactory.getLogger(XAStage.class);

    public static final String END_STAGE = "XA END STAGE";
    public static final String PREPARE_STAGE = "XA PREPARE STAGE";
    public static final String COMMIT_STAGE = "XA COMMIT STAGE";
    public static final String COMMIT_FAIL_STAGE = "XA COMMIT FAIL STAGE";
    public static final String ROLLBACK_STAGE = "XA ROLLBACK STAGE";
    public static final String ROLLBACK_FAIL_STAGE = "XA ROLLBACK FAIL STAGE";

    protected final NonBlockingSession session;
    protected AbstractXAHandler xaHandler;

    XAStage(NonBlockingSession session, AbstractXAHandler handler) {
        this.session = session;
        this.xaHandler = handler;
    }

    public abstract void onEnterStage(MySQLResponseService conn);

    @Override
    public void onEnterStage() {
        Set<RouteResultsetNode> resultsetNodes = xaHandler.setUnResponseRrns();
        for (RouteResultsetNode rrn : resultsetNodes) {
            if (null == session.getTarget(rrn)) {
                LOGGER.debug("this node may be release.{}", rrn);
            } else {
                final MySQLResponseService backendService = session.getTarget(rrn).getBackendService();
                if (backendService != null && !backendService.isFakeClosed()) {
                    onEnterStage(backendService);
                } else {
                    session.releaseConnection(rrn, false);
                    xaHandler.fakedResponse(rrn);
                }
            }
        }
    }

    protected void feedback(boolean isSuccess) {
        session.clearResources(false);
        if (session.closed()) {
            return;
        }

        if (this instanceof XACommitStage) session.setFinishedCommitTime();
        if (isSuccess && xaHandler.getTransactionCallback() != null) {
            xaHandler.getTransactionCallback().callback();
        }

        MySQLPacket sendData = xaHandler.getPacketIfSuccess();
        xaHandler.clearResources();
        if (sendData != null) {
            sendData.write(session.getSource());
        } else {
            session.getShardingService().writeOkPacket();
        }
    }

    // return ok
    public abstract void onConnectionOk(MySQLResponseService service);

    // connect error
    public abstract void onConnectionError(MySQLResponseService service, int errNo);

    // connect close
    public abstract void onConnectionClose(MySQLResponseService service);

    // connect error
    public abstract void onConnectError(MySQLResponseService service);

    public abstract String getStage();
}
