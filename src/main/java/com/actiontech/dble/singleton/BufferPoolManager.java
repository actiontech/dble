package com.actiontech.dble.singleton;

import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.buffer.DirectByteBufferPool;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.memory.unsafe.Platform;

import java.io.IOException;

/**
 * Created by szf on 2019/9/20.
 */
public final class BufferPoolManager {
    private static final BufferPoolManager INSTANCE = new BufferPoolManager();
    private BufferPool bufferPool;

    private BufferPoolManager() {

    }

    public static BufferPoolManager getInstance() {
        return INSTANCE;
    }

    public static BufferPool getBufferPool() {
        return INSTANCE.bufferPool;
    }

    public void init(SystemConfig system) throws IOException {
        // a page size
        int bufferPoolPageSize = system.getBufferPoolPageSize();
        // total page number
        short bufferPoolPageNumber = system.getBufferPoolPageNumber();
        //minimum allocation unit
        short bufferPoolChunkSize = system.getBufferPoolChunkSize();
        if ((long) bufferPoolPageSize * (long) bufferPoolPageNumber > Platform.getMaxDirectMemory()) {
            throw new IOException("Direct BufferPool size[bufferPoolPageSize(" + bufferPoolPageSize + ")*bufferPoolPageNumber(" + bufferPoolPageNumber + ")] larger than MaxDirectMemory[" + Platform.getMaxDirectMemory() + "]");
        }
        bufferPool = new DirectByteBufferPool(bufferPoolPageSize, bufferPoolChunkSize, bufferPoolPageNumber);
    }

}
