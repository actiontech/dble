/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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
