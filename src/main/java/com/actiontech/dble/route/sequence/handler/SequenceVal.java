/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by huqing.yan on 2017/7/3.
 */
public class SequenceVal {

    AtomicBoolean newValueSetted = new AtomicBoolean(false);
    AtomicLong curVal = new AtomicLong(0);
    volatile String dbretVal = null;
    volatile boolean dbfinished;
    AtomicBoolean fetching = new AtomicBoolean(false);
    volatile long maxSegValue;
    volatile boolean successFetched;
    volatile String dataNode;
    final String seqName;
    final String sql;

    public SequenceVal(String seqName, String dataNode) {
        this.seqName = seqName;
        this.dataNode = dataNode;
        sql = "SELECT dble_seq_nextval('" + seqName + "')";
    }

    public boolean isNexValValid(Long nexVal) {
        return nexVal < this.maxSegValue;
    }

    public void setCurValue(long newValue) {
        curVal.set(newValue);
        successFetched = true;
    }

    public Long[] waitFinish() {
        long start = System.currentTimeMillis();
        long end = start + 10 * 1000;
        while (System.currentTimeMillis() < end) {
            if (dbretVal == IncrSequenceMySQLHandler.ERR_SEQ_RESULT) {
                throw new java.lang.RuntimeException(
                        "sequnce not found in db table ");
            } else if (dbretVal != null) {
                String[] items = dbretVal.split(",");
                Long curValue = Long.parseLong(items[0]);
                int span = Integer.parseInt(items[1]);
                return new Long[]{curValue, curValue + span};
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    IncrSequenceMySQLHandler.LOGGER.warn("wait db fetch sequnce err " + e);
                }
            }
        }
        return null;
    }

    public boolean isSuccessFetched() {
        return successFetched;
    }

    public long nextValue() {
        if (!successFetched) {
            throw new java.lang.RuntimeException(
                    "sequnce fetched failed  from db ");
        }
        return curVal.incrementAndGet();
    }
}
