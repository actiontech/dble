/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.handler;

import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;

public final class CommitHandler {
    private CommitHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        service.commit(stmt);
    }
}
