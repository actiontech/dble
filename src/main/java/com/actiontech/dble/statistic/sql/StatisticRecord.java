package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.statistic.sql.entry.FrontendInfo;
import com.actiontech.dble.statistic.sql.entry.StatisticBackendSqlEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;
import com.actiontech.dble.statistic.sql.entry.StatisticTxEntry;

public class StatisticRecord {

    protected static final StatisticListener STATISTIC = StatisticListener.getInstance();
    protected FrontendInfo frontendInfo;

    // xa
    protected volatile String xaId = null;

    public void onXaStart(String id) {
        xaId = id;
    }

    public void onXaStop() {
        xaId = null;
    }

    // tx
    protected volatile StatisticTxEntry txEntry;
    protected volatile boolean isStartTx;

    public void setTxEntry(StatisticTxEntry txEntry) {
        this.txEntry = txEntry;
    }

    public void onTxPreStart() {
    }

    public void onTxStartBySet(BusinessService businessService) {
    }

    public void onTxStartByImplicitly(BusinessService businessService) {
        onTxStart(businessService, true, false);
    }

    public void onTxStart(BusinessService businessService) {
        onTxStart(businessService, false, false);
    }

    public void onTxStart(BusinessService businessService, boolean isImplicitly, boolean isSet) {
    }

    public void onTxEnd() {
    }

    public void onTxEndByExit() {
        if (isStartTx && txEntry != null) {
            long txEndTime = System.nanoTime();
            isStartTx = false;
            frontendSqlEntry = new StatisticFrontendSqlEntry(frontendInfo, txEndTime);
            frontendSqlEntry.setSql("exit");
            frontendSqlEntry.setAllEndTime(txEndTime);
            frontendSqlEntry.setXaId(xaId);
            frontendSqlEntry.setTxId(txid);
            txEntry.add(frontendSqlEntry);
            txEntry.setAllEndTime(txEndTime);
            pushTx();
        }
    }

    protected void onTxData(StatisticFrontendSqlEntry frontendSqlentry) {
        if (isStartTx && txEntry != null && txEntry.getTxId() > 0) {
            frontendSqlentry.setNeedToTx(false);
            if (frontendSqlentry.getTxId() == txEntry.getTxId()) {
                txEntry.add(frontendSqlentry);
            }
        }
    }

    // frontend sql
    protected volatile StatisticFrontendSqlEntry frontendSqlEntry;
    protected volatile boolean isStartFsql = false;
    protected volatile long txid;

    public void onFrontendSqlStart() {
        isStartFsql = true;
        frontendSqlEntry = new StatisticFrontendSqlEntry(frontendInfo, System.nanoTime());
        if (xaId != null) {
            frontendSqlEntry.setXaId(xaId);
        }
    }

    public void onFrontendSetSql(String schema, String sql) {
        if (isStartFsql && frontendSqlEntry != null) {
            if (sql == null || sql.toLowerCase().startsWith("explain")) {
                onFrontendSqlClose();
            } else {
                frontendSqlEntry.setSql(sql);
                frontendSqlEntry.setSchema(schema);
                if (isStartTx && txEntry != null) {
                    frontendSqlEntry.setTxId(txid);
                } else {
                    txid = STATISTIC.getIncrementVirtualTxID();
                    frontendSqlEntry.setTxId(txid);
                    frontendSqlEntry.setNeedToTx(true);
                }
            }
        }
    }

    public void onFrontendSqlClose() {
        if (isStartFsql && frontendSqlEntry != null) {
            if (frontendSqlEntry.getBackendSqlEntrys().size() <= 0) {
                isStartFsql = false;
                frontendSqlEntry = null;
            }
        }
    }

    public void onFrontendSetRows(long rows) {
        if (isStartFsql && frontendSqlEntry != null) {
            frontendSqlEntry.setRows(rows);
        }
    }

    public void onFrontendAddRows() {
        if (isStartFsql && frontendSqlEntry != null) {
            frontendSqlEntry.addRows();
        }
    }

    public void onFrontendSqlEnd() {
        if (isStartFsql && frontendSqlEntry != null) {
            if (frontendSqlEntry.getSql() == null) {
                onFrontendSqlClose();
            } else {
                frontendSqlEntry.setAllEndTime(System.nanoTime());
                isStartFsql = false;
                onTxData(frontendSqlEntry);
                pushFrontendSql();
            }
        }
    }

    // Backend sql
    public void onBackendSqlStart(MySQLResponseService service) {
    }

    public void onBackendSqlStart(BackendConnection connection) {
    }

    public void onBackendSqlFirstEnd(MySQLResponseService service) {
        if (isStartFsql && frontendSqlEntry != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null && frontendSqlEntry.getBackendSqlEntry(key).getFirstEndTime() == 0L) {
                frontendSqlEntry.getBackendSqlEntry(key).setFirstEndTime(System.nanoTime());
            }
        }
    }

    public void onBackendSqlSetRows(MySQLResponseService service, long rows) {
        if (isStartFsql && frontendSqlEntry != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null) {
                frontendSqlEntry.getBackendSqlEntry(key).setRows(rows);
                frontendSqlEntry.addExaminedRows(rows);
            }
        }
    }

    public void onBackendSqlAddRows(MySQLResponseService service) {
        if (isStartFsql && frontendSqlEntry != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null) {
                frontendSqlEntry.getBackendSqlEntry(key).addRows();
                frontendSqlEntry.addExaminedRows();
            }
        }
    }

    public void onBackendSqlEnd(MySQLResponseService service) {
        if (isStartFsql && frontendSqlEntry != null) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null) {
                frontendSqlEntry.getBackendSqlEntry(key).setAllEndTime(System.nanoTime());
                frontendSqlEntry.getBackendSqlEntry(key).setNeedToTx(frontendSqlEntry.isNeedToTx());
                pushBackendSql(frontendSqlEntry.getBackendSqlEntry(key));
            }
        }
    }

    public void onBackendSqlSetRowsAndEnd(long rows) {
    }

    public void onBackendSqlError(byte[] data) {
    }

    // push data
    protected void pushBackendSql(StatisticBackendSqlEntry backendSqlEntry) {
        if (backendSqlEntry != null) {
            StatisticManager.getInstance().push(backendSqlEntry);
        }
    }

    protected void pushFrontendSql() {
        if (frontendSqlEntry != null) {
            StatisticManager.getInstance().push(frontendSqlEntry);
            frontendSqlEntry = null;
        }
    }

    protected void pushTx() {
        if (txEntry != null) {
            if (txEntry.getEntryList().size() != 0) {
                StatisticManager.getInstance().push(txEntry);
            }
            txEntry = null;
        }
    }

    protected void pushXa(StatisticTxEntry xaEntry) {
        StatisticManager.getInstance().push(xaEntry);
    }

    public StatisticRecord(BusinessService service) {
        this.frontendInfo = new FrontendInfo(service.getUserConfig().getId(),
                service.getUser().getName(),
                service.getConnection().getHost(),
                service.getConnection().getPort());
    }
}
