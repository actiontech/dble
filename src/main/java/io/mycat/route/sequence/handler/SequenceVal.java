package io.mycat.route.sequence.handler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by huqing.yan on 2017/7/3.
 */
public class SequenceVal {

    public AtomicBoolean newValueSetted = new AtomicBoolean(false);
    public AtomicLong curVal = new AtomicLong(0);
    public volatile String dbretVal = null;
    public volatile boolean dbfinished;
    public AtomicBoolean fetching = new AtomicBoolean(false);
    public volatile long maxSegValue;
    public volatile boolean successFetched;
    public volatile String dataNode;
    public final String seqName;
    public final String sql;

    public SequenceVal(String seqName, String dataNode) {
        this.seqName = seqName;
        this.dataNode = dataNode;
        sql = "SELECT mycat_seq_nextval('" + seqName + "')";
    }

    public boolean isNexValValid(Long nexVal) {
        if (nexVal < this.maxSegValue) {
            return true;
        } else {
            return false;
        }
    }

//    FetchMySQLSequnceHandler seqHandler;

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
                Long curVal = Long.parseLong(items[0]);
                int span = Integer.parseInt(items[1]);
                return new Long[]{curVal, curVal + span};
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    IncrSequenceMySQLHandler.LOGGER
                            .warn("wait db fetch sequnce err " + e);
                }
            }
        }
        return null;
    }

    public boolean isSuccessFetched() {
        return successFetched;
    }

    public long nextValue() {
        if (successFetched == false) {
            throw new java.lang.RuntimeException(
                    "sequnce fetched failed  from db ");
        }
        return curVal.incrementAndGet();
    }
}
