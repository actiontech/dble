/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.sql.analyzer;

import com.actiontech.dble.statistic.sql.entry.FrontendInfo;
import com.actiontech.dble.statistic.sql.entry.StatisticFrontendSqlEntry;

/**
 * UserStat
 *
 * @author Ben
 */
public class UserStat {
    private FrontendInfo frontendInfo;
    private UserSqlRWStat sqlRwStat;

    public UserStat(FrontendInfo frontendInfo) {
        super();
        this.frontendInfo = frontendInfo;
        this.sqlRwStat = new UserSqlRWStat();
    }

    public FrontendInfo getFrontendInfo() {
        return frontendInfo;
    }

    public UserSqlRWStat getRWStat() {
        return sqlRwStat;
    }


    public void reset() {
        this.sqlRwStat.reset();
    }

    public void update(final StatisticFrontendSqlEntry fEntry) {
        //sqlRwStat
        this.sqlRwStat.add(fEntry.getSqlType(), fEntry.getDurationMs(), fEntry.getNetInBytes(), fEntry.getNetOutBytes(), fEntry.getEndTimeMs());

    }
}
