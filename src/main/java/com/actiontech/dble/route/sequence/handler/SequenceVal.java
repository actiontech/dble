/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by huqing.yan on 2017/7/3.
 */
public class SequenceVal {


    Counter counter = null;
    //exec fetch sql result
    volatile String dbretVal = null;
    //exec fetch sql flag
    volatile boolean dbfinished;
    //exec get next segment lock
    AtomicBoolean fetching = new AtomicBoolean(false);

    //flag if the init of the Sequence is done
    volatile boolean successFetched;
    //the dataNode of sequence creater
    volatile String dataNode;
    final String seqName;
    final String sql;
    private ReentrantLock executeLock = new ReentrantLock();
    private Condition condRelease = executeLock.newCondition();

    public SequenceVal(String seqName, String dataNode) {
        this.seqName = seqName;
        this.dataNode = dataNode;
        sql = "SELECT dble_seq_nextval('" + seqName + "')";
    }


    public void setNewCounter(long start, long end) {
        counter = new Counter(start, end);
        successFetched = true;
    }

    public Long[] waitFinish() {
        long start = System.currentTimeMillis();
        long end = start + 10 * 1000;
        while (System.currentTimeMillis() < end) {
            if (dbfinished) {
                if (dbretVal == null || IncrSequenceMySQLHandler.ERR_SEQ_RESULT.equals(dbretVal)) {
                    break;
                }
                String[] items = dbretVal.split(",");
                long curValue = Long.parseLong(items[0]);
                int span = Integer.parseInt(items[1]);
                return new Long[]{curValue, curValue + span};
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    IncrSequenceMySQLHandler.LOGGER.info("wait db fetch sequnce err " + e);
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
                    "sequence fetched failed  from db ");
        }
        return counter.getNext();
    }

    public void waitOtherFinish() {
        executeLock.lock();
        try {
            while (fetching.get()) {
                condRelease.await();
            }
        } catch (Exception e) {
            throw new java.lang.RuntimeException("wait");
        } finally {
            executeLock.unlock();
        }
    }


    public void signalAll() {
        executeLock.lock();
        try {
            this.fetching.set(false);
            condRelease.signalAll();
        } finally {
            executeLock.unlock();
        }
    }
}
