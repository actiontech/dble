/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.response.OnOffAlert;
import com.actiontech.dble.services.manager.response.OnOffCapClientFoundRows;
import com.actiontech.dble.services.manager.response.OnOffCustomMySQLHa;
import com.actiontech.dble.services.manager.response.OnOffSlowQueryLog;
import com.actiontech.dble.route.parser.ManagerParseOnOff;

public final class EnableHandler {
    private EnableHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        int rs = ManagerParseOnOff.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseOnOff.SLOW_QUERY_LOG:
                OnOffSlowQueryLog.execute(service, true);
                break;
            case ManagerParseOnOff.ALERT:
                OnOffAlert.execute(service, true);
                break;
            case ManagerParseOnOff.CUSTOM_MYSQL_HA:
                OnOffCustomMySQLHa.execute(service, true);
                break;
            case ManagerParseOnOff.CAP_CLIENT_FOUND_ROWS:
                OnOffCapClientFoundRows.execute(service, true);
                break;
            default:
                service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }

    }
}
