/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.route.parser.ManagerParseOnOff;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.services.manager.response.*;

public final class DisableHandler {
    private DisableHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        int rs = ManagerParseOnOff.parse(stmt, offset);
        switch (rs & 0xff) {
            case ManagerParseOnOff.SLOW_QUERY_LOG:
                OnOffSlowQueryLog.execute(service, false);
                break;
            case ManagerParseOnOff.ALERT:
                OnOffAlert.execute(service, false);
                break;
            case ManagerParseOnOff.CUSTOM_MYSQL_HA:
                OnOffCustomMySQLHa.execute(service, false);
                break;
            case ManagerParseOnOff.CAP_CLIENT_FOUND_ROWS:
                OnOffCapClientFoundRows.execute(service, false);
                break;
            case ManagerParseOnOff.GENERAL_LOG:
                GeneralLogCf.OnOffGeneralLog.execute(service, false);
                break;
            case ManagerParseOnOff.STATISTIC:
                StatisticCf.OnOff.execute(service, false);
                break;
            case ManagerParseOnOff.LOAD_DATA_BATCH:
                OnOffLoadDataBatch.execute(service, false);
                break;
            case ManagerParseOnOff.MEMORY_BUFFER_MONITOR:
                OnOffMemoryBufferMonitor.execute(service, false);
                break;
            case ManagerParseOnOff.SQLDUMP_SQL:
                SqlDumpLog.OnOff.execute(service, false);
                break;
            default:
                service.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}

