/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class QueryTimeCost {
    private volatile long requestTime = 0;
    private AtomicLong responseTime = new AtomicLong(0);
    private volatile Map<Long, QueryTimeCost> backEndTimeCosts;


    public long getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(long requestTime) {
        this.requestTime = requestTime;
    }

    public AtomicLong getResponseTime() {
        return responseTime;
    }
    public Map<Long, QueryTimeCost> getBackEndTimeCosts() {
        if (backEndTimeCosts == null) {
            backEndTimeCosts = new ConcurrentHashMap<>();
        }
        return backEndTimeCosts;
    }

}
