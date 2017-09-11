/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import com.actiontech.dble.server.ServerConnection;

public final class CommitHandler {
    private CommitHandler() {
    }

    public static void handle(String stmt, ServerConnection c) {
        c.commit(stmt);
    }
}
