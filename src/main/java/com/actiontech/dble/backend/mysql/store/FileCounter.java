/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.memory.environment.Hardware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class FileCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCounter.class);
    private static FileCounter fileCounter = new FileCounter();

    private final Lock lock;
    private final int maxFileSize;
    private int currentNum;

    private FileCounter() {
        this.lock = new ReentrantLock();
        long totalMem = Hardware.getSizeOfPhysicalMemory();
        long freeMem = Hardware.getFreeSizeOfPhysicalMemory();
        long currentMem = Math.min(totalMem / 2, freeMem);
        this.maxFileSize = (int) (currentMem / (SystemConfig.getInstance().getMappedFileSize() / 1024));
        LOGGER.info("current mem is " + currentMem + "kb. max file size is " + maxFileSize);
        this.currentNum = 0;
    }

    public static FileCounter getInstance() {
        return fileCounter;
    }

    public boolean increment() {
        lock.lock();
        try {
            if (this.currentNum >= maxFileSize)
                return false;
            this.currentNum++;
            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean decrement() {
        lock.lock();
        try {
            if (this.currentNum <= 0)
                return false;
            this.currentNum--;
            return true;
        } finally {
            lock.unlock();
        }
    }
}
