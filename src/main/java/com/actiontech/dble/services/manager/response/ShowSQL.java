/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.services.manager.ManagerService;


/**
 * Show Last SQL
 *
 * @author mycat
 * @author zhuam
 */
public final class ShowSQL {
    private ShowSQL() {
    }

    public static void execute(ManagerService service) {
        String sql = "select sql_id as ID, user as USER, start_time as START_TIME, duration as EXECUTE_TIME, sql_stmt as SQL from sql_log order by start_time desc";
        (new ManagerSelectHandler()).execute(service, sql);
    }
}
