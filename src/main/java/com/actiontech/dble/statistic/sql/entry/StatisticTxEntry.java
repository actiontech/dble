package com.actiontech.dble.statistic.sql.entry;

import java.util.ArrayList;
import java.util.List;

public class StatisticTxEntry extends StatisticEntry {
    private List<StatisticFrontendSqlEntry> frontendSqlEntry = new ArrayList<>();
    // 0：normal，1：xa

    //set： 0
    //start transaction & begin：1
    private int startType;
    private int endType;

    public StatisticTxEntry(FrontendInfo frontendInfo, String xaId, long txId, int startType, long startTime) {
        super(frontendInfo, txId, startTime);
        setXaId(xaId);
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

    public void setEndType(int type) {
        this.endType = type;
    }

    public int getEndType() {
        return endType;
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
