/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by szf on 2018/3/29.
 */
public class Counter {

    protected static final Logger LOGGER = LoggerFactory.getLogger(Counter.class);
    //Sequence Value of lastest
    AtomicLong curVal = new AtomicLong(0);
    //max seg value of this time,if the curVal equals this value than need to find a new segment
    long maxSegValue;

    public Counter(long start, long maxSegValue) {
        LOGGER.info("new counter " + start + "  " + maxSegValue);
        this.maxSegValue = maxSegValue;
        curVal = new AtomicLong(start);
    }

    public long getNext() {
        long value = curVal.incrementAndGet();

        return maxSegValue > value ? value : -1;
    }

}
