package com.actiontech.dble.server.handler;

import com.actiontech.dble.server.ServerConnection;

public final class CommitHandler {
    private CommitHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        c.commit(stmt);
    }
}
