package io.mycat.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

/**
 * 缓冲池
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 12:19 2016/5/23
 */
public interface BufferPool {
    ByteBuffer allocate();

    public ByteBuffer allocate(int size);

    public void recycle(ByteBuffer theBuf);

    public long capacity();

    public long size();

    public int getSharedOptsCount();

    public int getChunkSize();

    public ConcurrentMap<Long, Long> getNetDirectMemoryUsage();
}
