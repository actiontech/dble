package com.actiontech.dble.statistic.sql.entry;

import java.util.ArrayList;
import java.util.List;

public class StatisticTxEntry extends StatisticEntry {
    private List<StatisticFrontendSqlEntry> frontendSqlEntry = new ArrayList<>();
    private boolean isImplicitly;

    public StatisticTxEntry(FrontendInfo frontendInfo, String xaId, long txId, long startTime, boolean isImplicitly) {
        super(frontendInfo, txId, startTime);
        setXaId(xaId);
        this.isImplicitly = isImplicitly;
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

    public boolean isImplicitly() {
        return isImplicitly;
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
