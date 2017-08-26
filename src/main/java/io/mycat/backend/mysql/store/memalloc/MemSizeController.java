package io.mycat.backend.mysql.store.memalloc;

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
     * @param incre
     * @return reache limit?, if false:not ok, need flush to disk
     */
    public boolean addSize(long incre) {
        for (; ; ) {
            long current = size.get();
            long next = current + incre;
            if (size.compareAndSet(current, next)) {
                long minLeft = 32;
                return next + minLeft < maxSize;
            }
        }
    }

    public void subSize(long decre) {
        for (; ; ) {
            long current = size.get();
            long next = current - decre;
            if (next < 0) {
                throw new RuntimeException("unexpected!");
            }
            if (size.compareAndSet(current, next)) {
                return;
            }
        }
    }

}
