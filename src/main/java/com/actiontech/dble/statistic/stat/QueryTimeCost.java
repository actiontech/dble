/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class QueryTimeCost {
    private volatile long requestTime = 0;
    private AtomicLong responseTime = new AtomicLong(0);

    private AtomicInteger backendReserveCount = new AtomicInteger(0);
    private AtomicInteger backendExecuteCount = new AtomicInteger(0);
    private int backendSize = 0;

    private volatile Map<Long, QueryTimeCost> backEndTimeCosts;

    private AtomicBoolean firstBackConRes = new AtomicBoolean(false);

    private AtomicBoolean firstBackConEof = new AtomicBoolean(false);

    public QueryTimeCost() {
    }

    public void setCount(int x) {
        backendReserveCount.set(x);
        backendExecuteCount.set(x);
        backendSize = x;
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

    public AtomicInteger getBackendReserveCount() {
        return backendReserveCount;
    }

    public void setBackendReserveCount(AtomicInteger backendReserveCount) {
        this.backendReserveCount = backendReserveCount;
    }

    public AtomicInteger getBackendExecuteCount() {
        return backendExecuteCount;
    }

    public void setBackendExecuteCount(AtomicInteger backendExecuteCount) {
        this.backendExecuteCount = backendExecuteCount;
    }


    public int getBackendSize() {
        return backendSize;
    }

    public AtomicBoolean getFirstBackConEof() {
        return firstBackConEof;
    }

}
