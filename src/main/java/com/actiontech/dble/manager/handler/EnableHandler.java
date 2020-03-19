/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.manager.response.OnOffAlert;
import com.actiontech.dble.manager.response.OnOffSlowQueryLog;
import com.actiontech.dble.manager.response.OnOffCustomMySQLHa;
import com.actiontech.dble.route.parser.ManagerParseOnOff;

public final class EnableHandler {
    private EnableHandler() {
    }

    public static void handle(String stmt, ManagerConnection c, int offset) {
        int rs = ManagerParseOnOff.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseOnOff.SLOW_QUERY_LOG:
                OnOffSlowQueryLog.execute(c, true);
                break;
            case ManagerParseOnOff.ALERT:
                OnOffAlert.execute(c, true);
                break;
            case ManagerParseOnOff.CUSTOM_MYSQL_HA:
                OnOffCustomMySQLHa.execute(c, true);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }

    }
}
