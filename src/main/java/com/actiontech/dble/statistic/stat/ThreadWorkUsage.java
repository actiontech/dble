/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ThreadWorkUsage {
    private static final int STAT_PERIOD_1 = 15;
    private static final int STAT_PERIOD_2 = 60;
    private static final int STAT_PERIOD_3 = 300;
    private long currentSecondUsed;
    private LoopQueue lastStat1 = new LoopQueue(STAT_PERIOD_1);
    private LoopQueue lastStat2 = new LoopQueue(STAT_PERIOD_2);
    private LoopQueue lastStat3 = new LoopQueue(STAT_PERIOD_3);
    private ReentrantReadWriteLock currentLock = new ReentrantReadWriteLock();

    public long getCurrentSecondUsed() {
        currentLock.readLock().lock();
        try {
            return currentSecondUsed;
        } finally {
            currentLock.readLock().unlock();
        }
    }

    public void setCurrentSecondUsed(long currentSecondUsed) {
        currentLock.writeLock().lock();
        try {
            this.currentSecondUsed = currentSecondUsed;
        } finally {
            currentLock.writeLock().unlock();
        }
    }

    public String[] getUsedPercent() {
        currentLock.readLock().lock();
        try {
            return new String[]{lastStat1.getUsedPercent(), lastStat2.getUsedPercent(), lastStat3.getUsedPercent()};
        } finally {
            currentLock.readLock().unlock();
        }
    }

    public void switchToNew() {
        currentLock.writeLock().lock();
        try {
            long lastSecondUsed = currentSecondUsed;
            currentSecondUsed = 0;
            lastStat1.add(lastSecondUsed);
            lastStat2.add(lastSecondUsed);
            lastStat3.add(lastSecondUsed);
        } finally {
            currentLock.writeLock().unlock();
        }
    }

    private static class LoopQueue {
        private final int capacity;
        private long[] usedTime;
        private int index = 0;
        private boolean isFull = false;

        LoopQueue(int capacity) {
            this.capacity = capacity;
            usedTime = new long[capacity];
        }

        private void add(long value) {
            usedTime[index] = value;
            index++;
            if (index == capacity) {
                index = 0;
                isFull = true;
            }
        }

        private String getUsedPercent() {
            int length = index;
            if (isFull) {
                length = capacity;
            }
            long sumUsed = 0;
            for (int i = 0; i < length; i++) {
                sumUsed += usedTime[i];
            }
            long percent = length == 0 ? 0 : sumUsed / length / 10000000;
            return Long.toString(percent) + "%";
        }
    }
}
