package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.backend.mysql.nio.handler.MultiNodeDdlPrepareHandler;
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

    public void onExit(String reason) {
        long txEndTime = System.nanoTime();
        if (txEntry != null) {
            //if (reason.contains("quit cmd")) {
            StatisticFrontendSqlEntry f = new StatisticFrontendSqlEntry(frontendInfo, txEndTime);
            f.setSql("exit");
            f.setAllEndTime(txEndTime);
            f.setXaId(xaId);
            f.setTxId(txid);
            txEntry.add(f);
            //}
            txEntry.setAllEndTime(txEndTime);
            pushTx();
        } else {
            //if (!reason.contains("quit cmd")) return;
            StatisticFrontendSqlEntry f = new StatisticFrontendSqlEntry(frontendInfo, txEndTime);
            f.setSql("exit");
            f.setAllEndTime(txEndTime);
            f.setXaId(xaId);
            f.setTxId(txid);
            f.setNeedToTx(true);
            pushFrontendSql(f);
        }
    }

    protected void onTxData(StatisticFrontendSqlEntry frontendSqlentry) {
        if (txEntry != null && txEntry.getTxId() > 0 && frontendSqlentry.getSql() != null) {
            frontendSqlentry.setNeedToTx(false);
            if (frontendSqlentry.getTxId() == txEntry.getTxId()) {
                txEntry.add(frontendSqlentry);
            }
        }
    }

    // frontend sql
    protected volatile StatisticFrontendSqlEntry frontendSqlEntry;
    protected volatile long txid;

    public void onFrontendMultiSqlStart() {
    }

    public void onFrontendSqlStart() {
        if (frontendSqlEntry == null) {
            frontendSqlEntry = new StatisticFrontendSqlEntry(frontendInfo, System.nanoTime());
            if (xaId != null) {
                frontendSqlEntry.setXaId(xaId);
            }
        }
    }

    public void onFrontendSetSql(String schema, String sql) {
        if (frontendSqlEntry != null) {
            if (sql == null || sql.toLowerCase().startsWith("explain")) {
                onFrontendSqlClose();
            } else {
                frontendSqlEntry.setSql(sql.trim());
                frontendSqlEntry.setSchema(schema);
                if (txEntry != null) {
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
        frontendSqlEntry = null;
        //if (frontendSqlEntry != null) {
        //if (frontendSqlEntry.getBackendSqlEntrys().size() <= 0) {
        //frontendSqlEntry = null;
        //}
        //}
    }

    public void onFrontendSetRows(long rows) {
        if (frontendSqlEntry != null) {
            frontendSqlEntry.setRows(rows);
        }
    }

    public void onFrontendAddRows() {
        if (frontendSqlEntry != null) {
            frontendSqlEntry.addRows();
        }
    }

    public void onFrontendSqlEnd() {
        if (frontendSqlEntry != null) {
            if (frontendSqlEntry.getSql() == null) {
                onFrontendSqlClose();
            } else {
                frontendSqlEntry.setAllEndTime(System.nanoTime());
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
        if (frontendSqlEntry != null && isPassSql(service)) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null && frontendSqlEntry.getBackendSqlEntry(key).getFirstEndTime() == 0L) {
                frontendSqlEntry.getBackendSqlEntry(key).setFirstEndTime(System.nanoTime());
            }
        }
    }

    public void onBackendSqlSetRows(MySQLResponseService service, long rows) {
        if (frontendSqlEntry != null && isPassSql(service)) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null && !frontendSqlEntry.getBackendSqlEntry(key).isEnd()) {
                frontendSqlEntry.getBackendSqlEntry(key).setRows(rows);
                frontendSqlEntry.addExaminedRows(rows);
            }
        }
    }

    public void onBackendSqlAddRows(MySQLResponseService service) {
        if (frontendSqlEntry != null && isPassSql(service)) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null && !frontendSqlEntry.getBackendSqlEntry(key).isEnd()) {
                frontendSqlEntry.getBackendSqlEntry(key).addRows();
                frontendSqlEntry.addExaminedRows();
            }
        }
    }

    public void onBackendSqlEnd(MySQLResponseService service) {
        if (frontendSqlEntry != null && isPassSql(service)) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (frontendSqlEntry.getBackendSqlEntry(key) != null && !frontendSqlEntry.getBackendSqlEntry(key).isEnd()) {
                frontendSqlEntry.getBackendSqlEntry(key).setAllEndTime(System.nanoTime());
                frontendSqlEntry.getBackendSqlEntry(key).
                        setNeedToTx(frontendSqlEntry.isNeedToTx());
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
        if (backendSqlEntry != null && !backendSqlEntry.isEnd()) {
            backendSqlEntry.setEnd(true);
            if (backendSqlEntry.getSql() == null)
                return;
            StatisticManager.getInstance().push(backendSqlEntry);
        }
    }

    protected void pushFrontendSql() {
        if (frontendSqlEntry != null) {
            if (frontendSqlEntry.getSql() != null) {
                StatisticManager.getInstance().push(frontendSqlEntry);
                frontendSqlEntry = null;
            } else {
                onFrontendSqlClose();
            }
        }
    }

    public void pushFrontendSql(StatisticFrontendSqlEntry f) {
        if (f != null) {
            if (f.getSql() != null) {
                StatisticManager.getInstance().push(f);
            }
        }
    }

    protected void pushTx() {
        if (txEntry != null) {
            //if (txEntry.getEntryList().size() != 0) {
            StatisticManager.getInstance().push(txEntry);
            //}
            txEntry = null;
        }
    }

    protected void pushXa(StatisticTxEntry xaEntry) {
        StatisticManager.getInstance().push(xaEntry);
    }

    protected boolean isPassSql(MySQLResponseService service) {
        // Prepare SQL('select 1') issued by executing DDL statements does not participate in the statistics
        if (service.getResponseHandler() instanceof MultiNodeDdlPrepareHandler)
            return false;
        return true;
    }


    public StatisticRecord(BusinessService service) {
        this.frontendInfo = new FrontendInfo(service.getUserConfig().getId(),
                service.getUser().getName(),
                service.getConnection().getHost(),
                service.getConnection().getPort());
    }
}
