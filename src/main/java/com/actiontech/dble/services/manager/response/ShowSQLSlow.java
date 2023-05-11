/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.services.manager.ManagerService;

/**
 * show slow SQL, default TOP10
 *
 * @author mycat
 * @author zhuam
 */
public final class ShowSQLSlow {
    private ShowSQLSlow() {
    }

    public static void execute(ManagerService service) {
        long slowTime = (long) SlowQueryLog.getInstance().getSlowTime() * 1000000;
        String sql = "select user as USER, start_time as START_TIME, duration as EXECUTE_TIME, sql_stmt as SQL from sql_log where duration >= " + slowTime;
        (new ManagerSelectHandler()).execute(service, sql);
    }

}
