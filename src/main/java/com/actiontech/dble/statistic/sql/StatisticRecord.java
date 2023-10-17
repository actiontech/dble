/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.backend.mysql.nio.handler.ddl.MultiNodeDdlPrepareHandler;
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

    public void onExit(String reason) {
        long txEndTime = System.nanoTime();
        if (txEntry != null) {
            if (reason.contains("quit cmd")) {
                StatisticFrontendSqlEntry f = new StatisticFrontendSqlEntry(frontendInfo, txEndTime);
                f.setSql("exit");
                f.setAllEndTime(txEndTime);
                f.setXaId(xaId);
                f.setTxId(txid);
                txEntry.add(f);
            }
            txEntry.setAllEndTime(txEndTime);
            pushTx();
        } else {
            if (!reason.contains("quit cmd")) return;
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
        StatisticFrontendSqlEntry f = this.frontendSqlEntry;
        if (f != null) {
            if (sql == null || sql.toLowerCase().startsWith("explain")) {
                onFrontendSqlClose();
            } else {
                f.setSql(sql.trim());
                f.setSchema(schema);
                if (txEntry != null) {
                    f.setTxId(txid);
                } else {
                    txid = STATISTIC.getIncrementVirtualTxID();
                    f.setTxId(txid);
                    f.setNeedToTx(true);
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
        StatisticFrontendSqlEntry f = this.frontendSqlEntry;
        if (f != null) {
            f.setRows(rows);
        }
    }

    public void onFrontendAddRows() {
        StatisticFrontendSqlEntry f = this.frontendSqlEntry;
        if (f != null) {
            f.addRows();
        }
    }

    public void onFrontendSqlEnd() {
        StatisticFrontendSqlEntry f = this.frontendSqlEntry;
        if (f != null) {
            if (f.getSql() == null) {
                onFrontendSqlClose();
            } else {
                f.setAllEndTime(System.nanoTime());
                onTxData(f);
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
        StatisticFrontendSqlEntry f = this.frontendSqlEntry;
        if (f != null && isPassSql(service)) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (f.getBackendSqlEntry(key) != null && f.getBackendSqlEntry(key).getFirstEndTime() == 0L) {
                f.getBackendSqlEntry(key).setFirstEndTime(System.nanoTime());
            }
        }
    }

    public void onBackendSqlSetRows(MySQLResponseService service, long rows) {
        StatisticFrontendSqlEntry f = this.frontendSqlEntry;
        if (f != null && isPassSql(service)) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (f.getBackendSqlEntry(key) != null && !f.getBackendSqlEntry(key).isEnd()) {
                f.getBackendSqlEntry(key).setRows(rows);
                f.addExaminedRows(rows);
            }
        }
    }

    public void onBackendSqlAddRows(MySQLResponseService service) {
        StatisticFrontendSqlEntry f = this.frontendSqlEntry;
        if (f != null && isPassSql(service)) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (f.getBackendSqlEntry(key) != null && !f.getBackendSqlEntry(key).isEnd()) {
                f.getBackendSqlEntry(key).addRows();
                f.addExaminedRows();
            }
        }
    }

    public void onBackendSqlEnd(MySQLResponseService service) {
        StatisticFrontendSqlEntry f = this.frontendSqlEntry;
        if (f != null && isPassSql(service)) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            String key = service.getConnection().getId() + ":" + node.getName() + ":" + node.getStatementHash();
            if (f.getBackendSqlEntry(key) != null && !f.getBackendSqlEntry(key).isEnd()) {
                f.getBackendSqlEntry(key).setAllEndTime(System.nanoTime());
                f.getBackendSqlEntry(key).setNeedToTx(f.isNeedToTx());
                pushBackendSql(f.getBackendSqlEntry(key));
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
        StatisticFrontendSqlEntry f = this.frontendSqlEntry;
        if (f != null) {
            if (f.getSql() != null) {
                StatisticManager.getInstance().push(f);
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
