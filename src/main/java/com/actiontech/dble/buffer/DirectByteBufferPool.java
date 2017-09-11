/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DirectByteBufferPool
 *
 * @author wuzhih
 * @author zagnix
 */
@SuppressWarnings("restriction")
public class DirectByteBufferPool implements BufferPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectByteBufferPool.class);
    public static final String LOCAL_BUF_THREAD_PREX = "$_";
    private ByteBufferPage[] allPages;
    private final int chunkSize;
    // private int prevAllocatedPage = 0;
    private AtomicInteger prevAllocatedPage;
    private final int pageSize;
    private final short pageCount;
    /**
     * thread ID->the size of Direct Buffer
     */
    private final ConcurrentMap<Long, Long> memoryUsage;

    public DirectByteBufferPool(int pageSize, short chunkSize, short pageCount) {
        allPages = new ByteBufferPage[pageCount];
        this.chunkSize = chunkSize;
        this.pageSize = pageSize;
        this.pageCount = pageCount;
        prevAllocatedPage = new AtomicInteger(0);
        for (int i = 0; i < pageCount; i++) {
            allPages[i] = new ByteBufferPage(ByteBuffer.allocateDirect(pageSize), chunkSize);
        }
        memoryUsage = new ConcurrentHashMap<>();
    }

    /**
     * TODO expandBuffer...
     *
     * @param buffer
     * @return
     */
    public ByteBuffer expandBuffer(ByteBuffer buffer) {
        int oldCapacity = buffer.capacity();
        int newCapacity = oldCapacity << 1;
        ByteBuffer newBuffer = allocate(newCapacity);
        int newPosition = buffer.position();
        buffer.flip();
        newBuffer.put(buffer);
        newBuffer.position(newPosition);
        recycle(buffer);
        return newBuffer;
    }

    public ByteBuffer allocate() {
        return allocate(chunkSize);
    }

    public ByteBuffer allocate(int size) {
        final int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
        int selectedPage = prevAllocatedPage.incrementAndGet() % allPages.length;
        ByteBuffer byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
        if (byteBuf == null) {
            byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
        }
        final long threadId = Thread.currentThread().getId();

        if (byteBuf != null) {
            if (memoryUsage.containsKey(threadId)) {
                memoryUsage.put(threadId, memoryUsage.get(threadId) + byteBuf.capacity());
            } else {
                memoryUsage.put(threadId, (long) byteBuf.capacity());
            }
        }

        if (byteBuf == null) {
            return ByteBuffer.allocate(size);
        }
        return byteBuf;
    }

    public void recycle(ByteBuffer theBuf) {
        if (!(theBuf instanceof DirectBuffer)) {
            theBuf.clear();
            return;
        }

        final long size = theBuf.capacity();

        boolean recycled = false;
        DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
        int chunkCount = theBuf.capacity() / chunkSize;
        DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
        int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / this.chunkSize);
        for (ByteBufferPage allPage : allPages) {
            if ((recycled = allPage.recycleBuffer((ByteBuffer) parentBuf, startChunk, chunkCount))) {
                break;
            }
        }
        final long threadId = Thread.currentThread().getId();

        if (memoryUsage.containsKey(threadId)) {
            memoryUsage.put(threadId, memoryUsage.get(threadId) - size);
        }
        if (!recycled) {
            LOGGER.warn("warning ,not recycled buffer " + theBuf);
        }
    }

    private ByteBuffer allocateBuffer(int theChunkCount, int startPage, int endPage) {
        for (int i = startPage; i < endPage; i++) {
            ByteBuffer buffer = allPages[i].allocatChunk(theChunkCount);
            if (buffer != null) {
                prevAllocatedPage.getAndSet(i);
                return buffer;
            }
        }
        return null;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    @Override
    public ConcurrentMap<Long, Long> getNetDirectMemoryUsage() {
        return memoryUsage;
    }

    //TODO   should  fix it
    public long capacity() {
        return size();
    }

    public long size() {
        return (long) pageSize * chunkSize * pageCount;
    }

    //TODO
    public int getSharedOptsCount() {
        return 0;
    }


}
