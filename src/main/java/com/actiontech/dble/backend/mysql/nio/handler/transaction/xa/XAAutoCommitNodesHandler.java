/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa;

import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;

public class XAAutoCommitNodesHandler extends XACommitNodesHandler {
    private RouteResultsetNode[] nodes;

    public XAAutoCommitNodesHandler(NonBlockingSession session, byte[] packet, RouteResultsetNode[] nodes) {
        super(session);
        this.sendData = packet;
        this.nodes = nodes;
    }

    @Override
    protected void nextParse() {
        if (this.isFail()) {
            XAAutoRollbackNodesHandler autoHandler = new XAAutoRollbackNodesHandler(session, sendData, nodes, null);
            autoHandler.rollback();
        } else {
            commit();
        }
    }
}
