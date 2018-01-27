/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import java.util.HashMap;
import java.util.Map;

public class QueryTimeCost {
    private long requestTime = 0;
    private long responseTime = 0;
    private volatile Map<Long, QueryTimeCost> backEndTimeCosts;


    public long getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(long requestTime) {
        this.requestTime = requestTime;
    }

    public long getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(long responseTime) {
        this.responseTime = responseTime;
    }

    public Map<Long, QueryTimeCost> getBackEndTimeCosts() {
        if (backEndTimeCosts == null) {
            backEndTimeCosts = new HashMap<>();
        }
        return backEndTimeCosts;
    }

}
