package com.actiontech.dble.statistic.sql;

public class UsageData {

    private String dataTime;
    private String usage;

    public UsageData(String dataTime, String usage) {
        this.dataTime = dataTime;
        this.usage = usage;
    }

    public String getDataTime() {
        return dataTime;
    }

    public String getUsage() {
        return usage;
    }
}
