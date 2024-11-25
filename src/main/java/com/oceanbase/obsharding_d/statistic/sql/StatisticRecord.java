/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.ddl.MultiNodeDdlPrepareHandler;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.services.BusinessService;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.statistic.sql.entry.FrontendInfo;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticBackendSqlEntry;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticFrontendSqlEntry;
import com.oceanbase.obsharding_d.statistic.sql.entry.StatisticTxEntry;

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
    protected volatile StatisticTxEntry pushTxEntry;

    public void onTxStartByImplicitly() {
        onTxStart(true);
    }

    public void onTxStart() {
        onTxStart(false);
    }

    public void onTxStart(boolean isImplicitly) {
        if (isImplicitly) {
            txid = STATISTIC.getIncrementVirtualTxID();
        }
        txEntry = new StatisticTxEntry(frontendInfo, xaId, txid, System.nanoTime());
    }

    public void onTxEnd() {
        if (txEntry != null) {
            txEntry.setAllEndTime(System.nanoTime());
            txEntry.setCanPush(true);
            pushTxEntry = txEntry;
            txEntry = null;
        }
    }

    public void onExit(String reason) {
        long txEndTime = System.nanoTime();
        if (txEntry != null) {
            txEntry.setCanPush(true);
            pushTxEntry = txEntry;
            txEntry = null;
            if (reason.contains("quit cmd")) {
                StatisticFrontendSqlEntry f = new StatisticFrontendSqlEntry(frontendInfo, txEndTime);
                f.setSql("exit");
                f.setAllEndTime(txEndTime);
                f.setXaId(xaId);
                f.setTxId(txid);
                pushTxEntry.add(f);
            }
            pushTxEntry.setAllEndTime(txEndTime);
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

    protected boolean onTxData(StatisticFrontendSqlEntry frontendSqlentry) {
        boolean isPushTx = false;
        if (pushTxEntry != null && pushTxEntry.isCanPush()) {
            if (frontendSqlentry.getSql() != null) {
                frontendSqlentry.setNeedToTx(false);
                if (frontendSqlentry.getTxId() == pushTxEntry.getTxId()) {
                    pushTxEntry.add(frontendSqlentry);
                }
            } else {
                onFrontendSqlClose();
            }
            isPushTx = true;
        } else if (txEntry != null && txEntry.getTxId() > 0) {
            if (frontendSqlentry.getSql() != null) {
                frontendSqlentry.setNeedToTx(false);
                if (frontendSqlentry.getTxId() == txEntry.getTxId()) {
                    txEntry.add(frontendSqlentry);
                }
            }
        }
        return isPushTx;
    }

    // frontend sql
    protected volatile StatisticFrontendSqlEntry frontendSqlEntry;
    protected volatile long txid;

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
            f.setAllEndTime(System.nanoTime());
            boolean isPushTx = onTxData(f);
            pushFrontendSql();
            if (isPushTx)
                pushTx();
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

    public void onBackendSqlError(int errNo) {
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
        if (pushTxEntry != null) {
            StatisticManager.getInstance().push(pushTxEntry);
            pushTxEntry = null;
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
