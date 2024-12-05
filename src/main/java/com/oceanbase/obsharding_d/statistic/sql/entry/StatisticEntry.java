/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql.entry;

import com.oceanbase.obsharding_d.util.TimeUtil;

public class StatisticEntry {
    private FrontendInfo frontend;
    protected long rows = 0L;
    private long startTime = 0L;
    private long startTimeMs = 0L;
    private long allEndTime = 0L;
    private int txType; // 0-tx, 1-ax
    private long txId = -1L;
    protected String xaId;

    public StatisticEntry(FrontendInfo frontendInfo) {
        this.frontend = frontendInfo;
    }

    public StatisticEntry(FrontendInfo frontendInfo, long txId, long startTime) {
        this.frontend = frontendInfo;
        this.txId = txId;
        this.startTime = startTime;
        this.startTimeMs = TimeUtil.currentTimeMillis();
    }

    public StatisticEntry(FrontendInfo frontendInfo, long startTime) {
        this.frontend = frontendInfo;
        this.startTime = startTime;
        this.startTimeMs = TimeUtil.currentTimeMillis();
    }

    public void setTxId(long txId) {
        this.txId = txId;
    }

    public long getTxId() {
        return txId;
    }

    public String getXaId() {
        return xaId;
    }

    public void setXaId(String xaId) {
        if (xaId != null) {
            this.xaId = xaId;
            this.txType = 1;
        }
    }

    public int getTxType() {
        return txType;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public void addRows() {
        this.rows += 1;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getAllEndTime() {
        return allEndTime;
    }

    public void setAllEndTime(long allEndTime) {
        this.allEndTime = allEndTime;
    }

    public FrontendInfo getFrontend() {
        return frontend;
    }

    public long getDuration() {
        return allEndTime - startTime;
    }

    public static class BackendInfo {
        String name; // db_instance
        String host;
        int port;
        String node; // sharding_node

        BackendInfo(String name, String host, int port, String node) {
            this.name = name;
            this.host = host;
            this.port = port;
            this.node = node;
        }

        public String getName() {
            return name;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getNode() {
            return node;
        }
    }
}
