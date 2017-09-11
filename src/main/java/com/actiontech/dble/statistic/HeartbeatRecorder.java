/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.statistic;

import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Recorder avg time ,the default is 1,10,30 minutes.
 *
 * @author mycat
 */
public class HeartbeatRecorder {

    private static final int MAX_RECORD_SIZE = 256;
    private static final long AVG1_TIME = 60 * 1000L;
    private static final long AVG2_TIME = 10 * 60 * 1000L;
    private static final long AVG3_TIME = 30 * 60 * 1000L;
    private static final long SWAP_TIME = 24 * 60 * 60 * 1000L;

    private long avg1;
    private long avg2;
    private long avg3;
    private final Queue<Record> records;
    private final Queue<Record> recordsAll;

    private static final Logger LOGGER = LoggerFactory.getLogger("DataSourceSyncRecorder");

    public HeartbeatRecorder() {
        this.records = new ConcurrentLinkedQueue<>();
        this.recordsAll = new ConcurrentLinkedQueue<>();
    }

    public String get() {
        return String.valueOf(avg1) + ',' + avg2 + ',' + avg3;
    }

    public void set(long value) {
        try {
            long time = TimeUtil.currentTimeMillis();
            if (value < 0) {
                recordsAll.offer(new Record(0, time));
                return;
            }
            remove(time);
            int size = records.size();
            if (size == 0) {
                records.offer(new Record(value, time));
                avg1 = avg2 = avg3 = value;
                return;
            }
            if (size >= MAX_RECORD_SIZE) {
                records.poll();
            }
            records.offer(new Record(value, time));
            recordsAll.offer(new Record(value, time));
            calculate(time);
        } catch (Exception e) {
            LOGGER.error("record HeartbeatRecorder error ", e);
        }
    }

    /**
     * remove old data
     */
    private void remove(long time) {
        final Queue<Record> recordQueue = this.records;
        while (recordQueue.size() > 0) {
            Record record = recordQueue.peek();
            if (time >= record.time + AVG3_TIME) {
                recordQueue.poll();
            } else {
                break;
            }
        }

        final Queue<Record> recordsAllQueue = this.recordsAll;
        while (recordsAllQueue.size() > 0) {
            Record record = recordsAllQueue.peek();
            if (time >= record.time + SWAP_TIME) {
                recordsAllQueue.poll();
            } else {
                break;
            }
        }
    }

    private void calculate(long time) {
        long v1 = 0L, v2 = 0L, v3 = 0L;
        int c1 = 0, c2 = 0, c3 = 0;
        for (Record record : records) {
            long t = time - record.time;
            if (t <= AVG1_TIME) {
                v1 += record.value;
                ++c1;
            }
            if (t <= AVG2_TIME) {
                v2 += record.value;
                ++c2;
            }
            if (t <= AVG3_TIME) {
                v3 += record.value;
                ++c3;
            }
        }
        avg1 = (v1 / c1);
        avg2 = (v2 / c2);
        avg3 = (v3 / c3);
    }

    public Queue<Record> getRecordsAll() {
        return this.recordsAll;
    }

    /**
     * @author mycat
     */
    public static class Record {
        private long value;
        private long time;

        Record(long value, long time) {
            this.value = value;
            this.time = time;
        }

        public long getValue() {
            return this.value;
        }

        public void setValue(long value) {
            this.value = value;
        }

        public long getTime() {
            return this.time;
        }

        public void setTime(long time) {
            this.time = time;
        }


    }
}
