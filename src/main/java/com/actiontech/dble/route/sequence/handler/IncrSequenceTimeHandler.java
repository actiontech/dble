/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;

import com.actiontech.dble.route.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public final class IncrSequenceTimeHandler implements SequenceHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IncrSequenceTimeHandler.class);

    private static final String SEQUENCE_TIME_PROPS = "sequence_time_conf.properties";
    private static final IncrSequenceTimeHandler INSTANCE = new IncrSequenceTimeHandler();
    private IdWorker workey;


    public static IncrSequenceTimeHandler getInstance() {
        return IncrSequenceTimeHandler.INSTANCE;
    }

    private IncrSequenceTimeHandler() {
        load();
    }


    public void load() {
        // load sequnce properties
        Properties props = PropertiesUtil.loadProps(SEQUENCE_TIME_PROPS);

        long workid = Long.parseLong(props.getProperty("WORKID"));
        long dataCenterId = Long.parseLong(props.getProperty("DATAACENTERID"));

        workey = new IdWorker(workid, dataCenterId);
    }

    @Override
    public long nextId(String prefixName) {
        return workey.nextId();
    }


    /**
     * @author sw
     * <p>
     * Now:
     * 64 bit ID 30 (millisecond high 30 )+5(DATA_CENTER_ID)+5(WORKER_ID)+12(autoincrement)+12 (millisecond low 12)
     */
    static class IdWorker {
        private static final long TWEPOCH = 1288834974657L;
        private static final long TIMESTAMP_LOW_BITS = 12L;
        private static final long TIMESTAMP_LOW_MASK = 0xFFF;

        // bits for WORKER_ID
        private static final long WORKER_ID_BITS = 5L;
        // bits for DATA_CENTER_ID
        private static final long DATA_CENTER_ID_BITS = 5L;
        // MAX_WORKER_ID 31
        private static final long MAX_WORKER_ID = -1L ^ (-1L << WORKER_ID_BITS);
        // MAX_DATA_CENTER_ID 31
        private static final long MAX_DATA_CENTER_ID = -1L ^ (-1L << DATA_CENTER_ID_BITS);
        // bits for autoincrement
        private static final long SEQUENCE_BITS = 12L;

        private static final long SEQUENCE_SHIFT = TIMESTAMP_LOW_BITS;
        private static final long WORKER_ID_SHIFT = SEQUENCE_BITS + TIMESTAMP_LOW_BITS;
        private static final long DATACENTER_ID_SHIFT = WORKER_ID_BITS + SEQUENCE_BITS + TIMESTAMP_LOW_BITS;
        private static final long TIMESTAMP_HIGH_SHIFT = DATA_CENTER_ID_BITS + WORKER_ID_BITS + SEQUENCE_BITS + TIMESTAMP_LOW_BITS;

        private static final long SEQUENCE_MASK = -1L ^ (-1L << SEQUENCE_BITS);

        private static long lastTimestamp = -1L;

        private long sequence = 0L;
        private final long workerId;
        private final long datacenterId;

        IdWorker(long workerId, long datacenterId) {
            if (workerId > MAX_WORKER_ID || workerId < 0) {
                throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", MAX_WORKER_ID));
            }
            if (datacenterId > MAX_DATA_CENTER_ID || datacenterId < 0) {
                throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", MAX_DATA_CENTER_ID));
            }
            this.workerId = workerId;
            this.datacenterId = datacenterId;
        }

        public synchronized long nextId() {
            long timestamp = timeGen();
            if (timestamp < lastTimestamp) {
                try {
                    throw new Exception("Clock moved backwards.  Refusing to generate id for " +
                            (lastTimestamp - timestamp) + " milliseconds");
                } catch (Exception e) {
                    LOGGER.error("error", e);
                }
            }

            if (lastTimestamp == timestamp) {
                // in this millisecond
                sequence = (sequence + 1) & SEQUENCE_MASK;
                if (sequence == 0) {
                    // blocking util next millisecond
                    timestamp = tilNextMillis(lastTimestamp);
                }
            } else {
                sequence = 0;
            }
            lastTimestamp = timestamp;

            //42 bit timestamp, right shift 12 bit ,get high 30 bit,than left shift 34 bit
            long nextId = (((timestamp - TWEPOCH) >> TIMESTAMP_LOW_BITS) << TIMESTAMP_HIGH_SHIFT) |
                    (datacenterId << DATACENTER_ID_SHIFT) |
                    (workerId << WORKER_ID_SHIFT) |
                    (sequence << SEQUENCE_SHIFT) |
                    ((timestamp - TWEPOCH) & TIMESTAMP_LOW_MASK);

            return nextId;
        }

        private long tilNextMillis(final long lastStamp) {
            long timestamp = this.timeGen();
            while (timestamp <= lastStamp) {
                timestamp = this.timeGen();
            }
            return timestamp;
        }

        private long timeGen() {
            return System.currentTimeMillis();
        }
    }

}
