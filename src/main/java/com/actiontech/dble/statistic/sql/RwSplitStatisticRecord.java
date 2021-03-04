package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.backend.mysql.nio.MySQLInstance;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.statistic.sql.entry.StatisticBackendSqlEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticTxEntry;

public class RwSplitStatisticRecord extends StatisticRecord {

    public void onTxPreStart() {
        if (frontendSqlEntry != null) {
            frontendSqlEntry.setNeedToTx(false);
        }
    }

    public void onTxStartBySet(BusinessService businessService) {
        onTxStart(businessService, false, true);
    }

    public void onTxStart(BusinessService businessService, boolean isImplicitly, boolean isSet) {
        if (isImplicitly) {
            txid = STATISTIC.getIncrementVirtualTxID();
        }
        isStartTx = true;
        txEntry = new StatisticTxEntry(frontendInfo, xaId, txid, System.nanoTime(), isImplicitly);
        if (!isImplicitly && !isSet) {
            txEntry.add(frontendSqlEntry);
        }
    }

    public void onTxEnd() {
        if (isStartTx && txEntry != null) {
            long txEndTime = System.nanoTime();
            isStartTx = false;
            if (frontendSqlEntry != null && frontendSqlEntry.getAllEndTime() == 0L) {
                frontendSqlEntry.setAllEndTime(txEndTime);
                txEntry.add(frontendSqlEntry);
            }
            txEntry.setAllEndTime(txEndTime);
            pushTx();
        }
    }


    public void onBackendSqlStart(BackendConnection connection) {
        if (isStartFsql && frontendSqlEntry != null) {
            StatisticBackendSqlEntry entry = new StatisticBackendSqlEntry(
                    frontendInfo,
                    ((MySQLInstance) connection.getInstance()).getName(), connection.getHost(), connection.getPort(), "-",
                    frontendSqlEntry.getSql(), System.nanoTime());
            frontendSqlEntry.put("&statistic_rw_key", entry);
        }
    }

    public void onBackendSqlSetRowsAndEnd(long rows) {
        if (isStartFsql && frontendSqlEntry != null) {
            frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key").setRows(rows);
            frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key").setAllEndTime(System.nanoTime());
            frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key").setNeedToTx(frontendSqlEntry.isNeedToTx());
            frontendSqlEntry.setRowsAndExaminedRows(rows);
            pushBackendSql(frontendSqlEntry.getBackendSqlEntry("&statistic_rw_key"));
            onFrontendSqlEnd();
        }
    }

    public void onBackendSqlError(byte[] data) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(data);
        if (errPg.getErrNo() == ErrorCode.ER_PARSE_ERROR ||
                errPg.getErrNo() == ErrorCode.ER_NO_SUCH_TABLE ||
                errPg.getErrNo() == ErrorCode.ER_NO_DB_ERROR ||
                errPg.getErrNo() == ErrorCode.ER_BAD_DB_ERROR ||
                errPg.getErrNo() == ErrorCode.ER_DERIVED_MUST_HAVE_ALIAS) {
            onFrontendSqlClose();
            return;
        }
        onBackendSqlSetRowsAndEnd(0);
    }

    protected void pushFrontendSql() {
        if (frontendSqlEntry != null) {
            StatisticManager.getInstance().push(frontendSqlEntry);
        }
    }

    public RwSplitStatisticRecord(BusinessService service) {
        super(service);
    }
}
