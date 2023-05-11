/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.services.manager.ManagerService;

/**
 * ShowSQLLarge
 *
 * @author zhuam
 */
public final class ShowSQLLarge {
    private ShowSQLLarge() {
    }

    public static void execute(ManagerService service) {
        String sql = "select user as USER, rows as ROWS, start_time as START_TIME, duration as EXECUTE_TIME, sql_stmt as SQL from dble_information.sql_log where sql_type='Select' and rows > 10000;";
        (new ManagerSelectHandler()).execute(service, sql);
    }
}

