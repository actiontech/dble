/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal;

import com.actiontech.dble.server.NonBlockingSession;

public class NormalAutoCommitNodesHandler extends NormalCommitNodesHandler {
    public NormalAutoCommitNodesHandler(NonBlockingSession session, byte[] packet) {
        super(session);
        this.sendData = packet;
    }

    @Override
    protected void setResponseTime(boolean isSuccess) {
        session.setFinishedCommitTime();
        session.setResponseTime(isSuccess);
    }
}
