/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.entry;

public final class StatisticBackendSqlEntry extends StatisticEntry {
    private BackendInfo backend;
    private boolean isNeedToTx;

    public StatisticBackendSqlEntry(
            FrontendInfo frontendInfo,
            BackendInfo backendInfo, long startTime,
            String sql, int sqlType, long rows, long endTime) {
        super(frontendInfo, startTime, sql, rows, endTime);
        this.backend = backendInfo;
        this.sqlType = sqlType;
    }

    public BackendInfo getBackend() {
        return backend;
    }

    public String getSql() {
        return sql;
    }

    public boolean isNeedToTx() {
        return isNeedToTx;
    }

    public void setNeedToTx(boolean needToTx) {
        isNeedToTx = needToTx;
    }

    public String getKey() {
        StringBuffer key = new StringBuffer();
        key.append(getFrontend().getUserId());
        key.append(":");
        key.append(getFrontend().getUser());
        key.append(":");
        key.append(getFrontend().getHost());
        key.append("|");
        key.append(getBackend().getNode());
        key.append(":");
        key.append(getBackend().getName());
        key.append(":");
        key.append(getBackend().getHost());
        key.append(":");
        key.append(getBackend().getPort());
        return key.toString();
    }
}
