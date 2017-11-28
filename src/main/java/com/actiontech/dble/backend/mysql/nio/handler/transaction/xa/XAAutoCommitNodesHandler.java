/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

import java.util.ArrayList;
import java.util.List;

public class XAAutoCommitNodesHandler extends XACommitNodesHandler {
    private RouteResultsetNode[] nodes;

    public XAAutoCommitNodesHandler(NonBlockingSession session, byte[] packet, RouteResultsetNode[] nodes) {
        super(session);
        this.sendData = packet;
        this.nodes = nodes;
    }

    @Override
    public boolean init() {
        boolean isNormal = true;
        List<BackendConnection> errConnection = null;
        for (RouteResultsetNode rrn : session.getTargetKeys()) {
            BackendConnection conn = session.getTarget(rrn);
            conn.setResponseHandler(this);
            if (conn.isClosedOrQuit()) {
                if (errConnection == null) {
                    errConnection = new ArrayList<>(1);
                }
                errConnection.add(conn);
                isNormal = false;
            }
        }
        if (isNormal) {
            return true;
        } else {
            String reason = "backend conn closed";
            sendData = makeErrorPacket(reason);
            autoRollback(errConnection);
            return false;
        }
    }

    private void autoRollback(List<BackendConnection> errConnection) {
        XAAutoRollbackNodesHandler autoHandler = new XAAutoRollbackNodesHandler(session, sendData, nodes, errConnection);
        autoHandler.rollback();
    }

    @Override
    protected void nextParse() {
        if (this.isFail()) {
            autoRollback(null);
        } else {
            commit();
        }
    }
}
