/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.buffer;

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
    final MemoryBufferMonitor bufferPoolMonitor = MemoryBufferMonitor.getInstance();

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


    public ByteBuffer allocate(BufferPoolRecord.Builder bufferRecordBuilder) {
        return allocate(chunkSize, bufferRecordBuilder);
    }

    public ByteBuffer allocate(int size, BufferPoolRecord.Builder bufferRecordBuilder) {
        final int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
        int selectedPage = (int) (prevAllocatedPage.incrementAndGet() % allPages.length);
        ByteBuffer byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
        if (byteBuf == null) {
            byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
        }
        if (byteBuf != null) {
            bufferPoolMonitor.addRecord(bufferRecordBuilder, ((DirectBuffer) byteBuf).address(), size);
        }

        if (byteBuf == null) {
            int allocatedSize = theChunkCount * chunkSize;
            if (allocatedSize > pageSize) {
                LOGGER.warn("You may need to turn up page size. The maximum size of the DirectByteBufferPool that can be allocated at one time is {}, and the size that you would like to allocate is {}", pageSize, size);
            } else {
                LOGGER.warn("Please pay attention to whether it is a memory leak. The maximum size of the DirectByteBufferPool that can be allocated at one time is {}, and the size that you would like to allocate is {}", pageSize, size);
            }
            return ByteBuffer.allocate(allocatedSize);
        }
        return byteBuf;
    }


    @Override
    public void recycle(ByteBuffer theBuf) {
        if (!(theBuf.isDirect())) {
            theBuf.clear();
            return;
        }

        bufferPoolMonitor.remove(((DirectBuffer) theBuf).address());


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
