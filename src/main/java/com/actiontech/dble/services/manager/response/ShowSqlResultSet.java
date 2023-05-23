/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.services.manager.ManagerService;

/**
 * ShowSqlResultSet
 *
 * @author songgw
 */
public final class ShowSqlResultSet {
    private ShowSqlResultSet() {
    }

    public static void execute(ManagerService service) {
        String sql = "select sql_id as ID, user as USER, t2.FREQUENCY, sql_stmt as SQL, result_size as RESULT_SIZE from dble_information.sql_log t1" +
                " inner join (" +
                " select max(sql_id) as maxId, count(0) as FREQUENCY from dble_information.sql_log group by sql_digest having result_size >= " + SystemConfig.getInstance().getMaxResultSet() + " order by maxId" +
                " ) t2" +
                " on  t1.sql_id = t2.maxId";
        (new ManagerSelectHandler()).execute(service, sql);
    }
}
