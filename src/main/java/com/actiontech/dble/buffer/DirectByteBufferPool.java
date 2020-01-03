/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

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
    private AtomicLong prevAllocatedPage;
    private final int pageSize;
    private final short pageCount;

    public DirectByteBufferPool(int pageSize, short chunkSize, short pageCount) {
        allPages = new ByteBufferPage[pageCount];
        this.chunkSize = chunkSize;
        this.pageSize = pageSize;
        this.pageCount = pageCount;
        prevAllocatedPage = new AtomicLong(0);
        for (int i = 0; i < pageCount; i++) {
            allPages[i] = new ByteBufferPage(ByteBuffer.allocateDirect(pageSize), chunkSize);
        }
    }

    /**
     * TODO expandBuffer...
     *
     * @param  buffer ByteBuffer
     * @return ByteBuffer
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
        int selectedPage = (int) (prevAllocatedPage.incrementAndGet() % allPages.length);
        ByteBuffer byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
        if (byteBuf == null) {
            byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
        }

        if (byteBuf == null) {
            LOGGER.warn("can't allocate DirectByteBuffer from DirectByteBufferPool. Please pay attention to whether it is a memory leak or there is no enough direct memory.");
            return ByteBuffer.allocate(size);
        }
        return byteBuf;
    }

    public void recycle(ByteBuffer theBuf) {
        if (!(theBuf instanceof DirectBuffer)) {
            theBuf.clear();
            return;
        }

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
        if (!recycled) {
            LOGGER.info("warning ,not recycled buffer " + theBuf);
        }
    }

    private ByteBuffer allocateBuffer(int theChunkCount, int startPage, int endPage) {
        for (int i = startPage; i < endPage; i++) {
            ByteBuffer buffer = allPages[i].allocateChunk(theChunkCount);
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

    /**
     * return the total size of the buffer memory
     *
     * @return long
     */
    public long capacity() {
        return (long) pageSize * pageCount;
    }

    /**
     * return the remain free part of memory
     *
     * @return long
     */
    public long size() {
        long usage = 0L;
        for (ByteBufferPage page : allPages) {
            usage += page.getUsage();
        }
        return this.capacity() - usage;
    }

    //TODO
    public int getSharedOptsCount() {
        return 0;
    }


}
