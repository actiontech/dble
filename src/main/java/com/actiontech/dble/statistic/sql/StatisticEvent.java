package com.actiontech.dble.statistic.sql;

import com.actiontech.dble.statistic.sql.entry.StatisticEntry;

public class StatisticEvent {

    private StatisticEntry entry;

    public void setEntry(StatisticEntry entry) {
        this.entry = entry;
    }

    public StatisticEntry getEntry() {
        return entry;
    }
}
