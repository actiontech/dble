/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.memalloc;

import java.util.concurrent.atomic.AtomicLong;

/**
 * MemSizeController
 *
 * @author ActionTech
 * @CreateTime 2016/1/19
 */
public class MemSizeController {
    private AtomicLong size;
    private long maxSize;

    public MemSizeController(long maxSize) {
        this.size = new AtomicLong();
        this.maxSize = maxSize;
    }

    /**
     * addSize
     *
     * @param increment
     * @return reached limit?, if false:not ok, need flush to disk
     */
    public boolean addSize(long increment) {
        for (; ; ) {
            long current = size.get();
            long next = current + increment;
            if (size.compareAndSet(current, next)) {
                long minLeft = 32;
                return next + minLeft < maxSize;
            }
        }
    }

    public void subSize(long decrement) {
        for (; ; ) {
            long current = size.get();
            long next = current - decrement;
            if (next < 0) {
                throw new RuntimeException("unexpected!");
            }
            if (size.compareAndSet(current, next)) {
                return;
            }
        }
    }

}
