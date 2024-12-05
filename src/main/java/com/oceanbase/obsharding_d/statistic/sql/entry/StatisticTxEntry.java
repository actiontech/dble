/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql.entry;

import java.util.ArrayList;
import java.util.List;

public class StatisticTxEntry extends StatisticEntry {
    private List<StatisticFrontendSqlEntry> frontendSqlEntry = new ArrayList<>();
    private boolean isCanPush = false;

    public StatisticTxEntry(FrontendInfo frontendInfo, String xaId, long txId, long startTime) {
        super(frontendInfo, txId, startTime);
        setXaId(xaId);
    }

    public void add(final StatisticFrontendSqlEntry entry) {
        frontendSqlEntry.add(entry);
    }

    public void clear() {
        frontendSqlEntry.clear();
    }

    public List<StatisticFrontendSqlEntry> getEntryList() {
        return frontendSqlEntry;
    }

    public boolean isCanPush() {
        return isCanPush;
    }

    public void setCanPush(boolean canPush) {
        isCanPush = canPush;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("StatisticTxEntry==>[");
        sb.append("txId='" + getTxId() + "',");
        sb.append("frontend=[userId=" + getFrontend().getUserId() + ",user=" + getFrontend().getUser() + ",host&port=" + getFrontend().getHost() + ":" + getFrontend().getPort() + "]");
        sb.append("time=[start=" + getStartTime() + ",end=" + getAllEndTime() + "]");
        return sb.toString();
    }
}
