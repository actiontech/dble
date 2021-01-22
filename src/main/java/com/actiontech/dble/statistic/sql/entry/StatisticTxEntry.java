package com.actiontech.dble.statistic.sql.entry;

import java.util.ArrayList;
import java.util.List;

public class StatisticTxEntry extends StatisticEntry {
    private List<StatisticFrontendSqlEntry> frontendSqlEntry = new ArrayList<>();
    //set： 0
    //start transaction & begin：1
    private int startType = 0;
    private int endType = 0;

    public StatisticTxEntry(FrontendInfo frontendInfo, long txId, int startType, long startTime) {
        super(frontendInfo, -1, txId, startTime);
        this.startType = startType;
    }

    public void add(StatisticFrontendSqlEntry entry) {
        frontendSqlEntry.add(entry);
    }

    public void clear() {
        frontendSqlEntry.clear();
    }

    public List<StatisticFrontendSqlEntry> getEntryList() {
        return frontendSqlEntry;
    }

    public int getStartType() {
        return startType;
    }

    public void setEndType(int endType) {
        this.endType = endType;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("StatisticTxEntry==>[");
        sb.append("txId='" + getTxId() + "',");
        sb.append("frontend=[user=" + getFrontend().getUser() + ",host&port=" + getFrontend().getHost() + ":" + getFrontend().getPort() + "]");
        sb.append("time=[start=" + getStartTime() + ",end=" + getAllEndTime() + "]");
        return sb.toString();
    }
}
