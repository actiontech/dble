/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.services.manager.ManagerService;

/**
 * ShowSQLHigh
 *
 * @author zhuam
 */
public final class ShowSQLHigh {
    private ShowSQLHigh() {
    }

    public static void execute(ManagerService service) {
        String sql = "select sql_id as ID, user as USER, count(0) as FREQUENCY, avg(duration) AVG_TIME, max(duration) as MAX_TIME, min(duration) as MIN_TIME, duration as EXECUTE_TIME, start_time as LAST_TIME, sql_digest as SQL from sql_log group by sql_digest order by start_time";
        (new ManagerSelectHandler()).execute(service, sql);
    }

}
