/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql;

import com.oceanbase.obsharding_d.backend.mysql.nio.MySQLInstance;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.services.BusinessService;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticBackendSqlEntry;

public class RwSplitStatisticRecord extends StatisticRecord {

    public void onBackendSqlStart(BackendConnection connection) {
        if (frontendSqlEntry != null) {
            StatisticBackendSqlEntry entry = new StatisticBackendSqlEntry(
                    frontendInfo,
                    ((MySQLInstance) connection.getInstance()).getName(), connection.getHost(), connection.getPort(), "-",
                    frontendSqlEntry.getSql(), System.nanoTime());
            frontendSqlEntry.put("&statistic_rw_key", entry);
        }
    }

    public void onBackendSqlSetRowsAndEnd(long rows) {
        if (frontendSqlEntry != null) {
            if (frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key") == null)
                return;
            frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key").setRows(rows);
            frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key").setAllEndTime(System.nanoTime());
            frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key").setNeedToTx(frontendSqlEntry.isNeedToTx());
            frontendSqlEntry.setRowsAndExaminedRows(rows);
            pushBackendSql(frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key"));
        }
    }

    public void onBackendSqlError(int errNo) {
        if (errNo == ErrorCode.ER_PARSE_ERROR) {
            onFrontendSqlClose();
            return;
        }
        onBackendSqlSetRowsAndEnd(0);
    }

    public RwSplitStatisticRecord(BusinessService service) {
        super(service);
    }
}
