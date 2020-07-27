/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.config.model.SystemConfig;

import java.util.concurrent.atomic.AtomicInteger;

public final class QueryTimeCostContainer {
    private final int maxSize;
    private final AtomicInteger index = new AtomicInteger(0);
    private static final QueryTimeCostContainer INSTANCE = new QueryTimeCostContainer();

    public QueryTimeCost[] getRecorders() {
        return recorders;
    }

    private QueryTimeCost[] recorders;

    public int getRealPos() {
        return realPos;
    }

    private int realPos = -1;

    public static QueryTimeCostContainer getInstance() {
        return INSTANCE;
    }

    private QueryTimeCostContainer() {
        this.maxSize = SystemConfig.getInstance().getMaxCostStatSize();
        recorders = new QueryTimeCost[maxSize];
    }

    public void add(QueryTimeCost e) {
        int startIndex = index.getAndIncrement();
        if (startIndex < 0) {
            synchronized (index) {
                if (index.get() < 0) {
                    index.set(realPos);
                }
                startIndex = index.incrementAndGet();
            }
        }
        if (startIndex >= maxSize) {
            realPos = startIndex % maxSize;
        } else {
            realPos = startIndex;
        }
        recorders[realPos] = e;
    }
}
