/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.statistic.sql;

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
