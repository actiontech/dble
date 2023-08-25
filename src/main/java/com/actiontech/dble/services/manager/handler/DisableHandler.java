/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.parser.ManagerParseOnOff;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.response.*;

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
            case ManagerParseOnOff.STATISTIC_ANALYSIS:
                StatisticCf.AnalysisOnOff.execute(service, false);
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

