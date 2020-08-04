/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.sequence.handler;

import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;

public final class IncrSequenceTimeHandler implements SequenceHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(IncrSequenceTimeHandler.class);

    private IdWorker workey;

    public void load(boolean isLowerCaseTableNames) {
        long startTimeMilliseconds = ClusterConfig.getInstance().sequenceStartTime();
        workey = new IdWorker(startTimeMilliseconds);
    }

    @Override
    public long nextId(String prefixName) throws SQLNonTransientException {
        return workey.nextId();
    }


    /**
     * @author sw
     *         <p>
     *         Now:
     *         64 bit ID 30 (millisecond high 30 )+10(instance_ID)+12(autoincrement)+12 (millisecond low 12)
     */
    static class IdWorker {
        private static final long TIMESTAMP_LOW_BITS = 12L;
        private static final long TIMESTAMP_LOW_MASK = 0xFFF;

        // bits for INSTANCE_ID
        private static final long INSTANCE_ID_BITS = 10L;
        // MAX_INSTANCE_ID 1023
        private static final long MAX_INSTANCE_ID = ~(-1L << INSTANCE_ID_BITS);
        // bits for autoincrement
        private static final long SEQUENCE_BITS = 12L;

        private static final long SEQUENCE_SHIFT = TIMESTAMP_LOW_BITS;
        private static final long INSTANCE_ID_SHIFT = SEQUENCE_BITS + TIMESTAMP_LOW_BITS;
        private static final long TIMESTAMP_HIGH_SHIFT = INSTANCE_ID_BITS + SEQUENCE_BITS + TIMESTAMP_LOW_BITS;

        private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

        private long lastTimestamp;
        private final long instanceId;
        private final long startTimeMillisecond;
        private final long deadline;
        private long sequence = 0L;

        IdWorker(long startTimeMillisecond) {
            if (SystemConfig.getInstance().getInstanceId() > MAX_INSTANCE_ID || SystemConfig.getInstance().getInstanceId() < 0) {
                throw new IllegalArgumentException(String.format("instanceId can't be greater than %d or less than 0", MAX_INSTANCE_ID));
            }
            this.instanceId = SystemConfig.getInstance().getInstanceId();
            this.startTimeMillisecond = startTimeMillisecond;
            this.lastTimestamp = startTimeMillisecond;
            this.deadline = startTimeMillisecond + (1L << 41);
        }

        public synchronized long nextId() throws SQLNonTransientException {
            long timestamp = timeGen();
            if (timestamp < lastTimestamp) {
                throw new SQLNonTransientException("Clock moved backwards.  Refusing to generate id for " +
                        (lastTimestamp - timestamp) + " milliseconds");
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

            if (timestamp >= deadline) {
                throw new SQLNonTransientException("Global sequence has reach to max limit and can generate duplicate sequences.");
            }
            lastTimestamp = timestamp;
            //42 bit timestamp, right shift 12 bit ,get high 30 bit,than left shift 34 bit
            return (((timestamp - startTimeMillisecond) >> TIMESTAMP_LOW_BITS) << TIMESTAMP_HIGH_SHIFT) |
                    (instanceId << INSTANCE_ID_SHIFT) |
                    (sequence << SEQUENCE_SHIFT) |
                    ((timestamp - startTimeMillisecond) & TIMESTAMP_LOW_MASK);
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
