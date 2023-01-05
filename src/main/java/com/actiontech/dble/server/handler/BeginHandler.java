/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.handler;

import com.actiontech.dble.services.mysqlsharding.ShardingService;

public final class BeginHandler {
    private BeginHandler() {
    }

    public static void handle(String stmt, ShardingService service) {
        service.begin(stmt);
    }
}
