/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.statistic.sql.entry.StatisticBackendSqlEntry;

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

    public void onBackendSqlError(byte[] data) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(data);
        if (errPg.getErrNo() == ErrorCode.ER_PARSE_ERROR) {
            onFrontendSqlClose();
            return;
        }
        onBackendSqlSetRowsAndEnd(0);
    }

    public RwSplitStatisticRecord(BusinessService service) {
        super(service);
    }
}
