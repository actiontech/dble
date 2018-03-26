/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class QueryTimeCost {
    private volatile long requestTime = 0;
    private AtomicLong responseTime = new AtomicLong(0);

    private AtomicLong backendReserveCount = new AtomicLong(0);
    private AtomicLong backendExecuteCount = new AtomicLong(0);

    private volatile Map<Long, QueryTimeCost> backEndTimeCosts;

    private AtomicBoolean firstBackConRes = new AtomicBoolean(false);

    public QueryTimeCost() {
    }

    public void setCount(int x) {
        backendReserveCount.set(x);
        backendExecuteCount.set(x);
    }

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

    public AtomicBoolean getFirstBackConRes() {
        return firstBackConRes;
    }

    public AtomicLong getBackendReserveCount() {
        return backendReserveCount;
    }

    public void setBackendReserveCount(AtomicLong backendReserveCount) {
        this.backendReserveCount = backendReserveCount;
    }

    public AtomicLong getBackendExecuteCount() {
        return backendExecuteCount;
    }

    public void setBackendExecuteCount(AtomicLong backendExecuteCount) {
        this.backendExecuteCount = backendExecuteCount;
    }
}
