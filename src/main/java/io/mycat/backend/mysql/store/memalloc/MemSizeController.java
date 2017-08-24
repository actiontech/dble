package io.mycat.backend.mysql.store.memalloc;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存使用大小控制器
 *
 * @author ActionTech
 * @CreateTime 2016年1月19日
 */
public class MemSizeController {
    // 当前内存大小
    private AtomicLong size;
    private long maxSize;

    public MemSizeController(long maxSize) {
        this.size = new AtomicLong();
        this.maxSize = maxSize;
    }

    /**
     * 增加了大小
     *
     * @param incre
     * @return 是否已经到达内存控制器极限, true:当前是ok的, false:not ok, need flush to disk
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
