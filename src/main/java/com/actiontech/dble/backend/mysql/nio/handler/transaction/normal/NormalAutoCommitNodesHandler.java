package com.actiontech.dble.backend.mysql.nio.handler.transaction.normal;

import com.actiontech.dble.server.NonBlockingSession;

public class NormalAutoCommitNodesHandler extends NormalCommitNodesHandler {
    public NormalAutoCommitNodesHandler(NonBlockingSession session, byte[] packet) {
        super(session);
        this.sendData = packet;
    }
}
