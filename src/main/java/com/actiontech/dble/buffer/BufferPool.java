/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.buffer;

import java.nio.ByteBuffer;

/**
 * BufferPool
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 12:19 2016/5/23
 */
public interface BufferPool {
    ByteBuffer allocate(BufferPoolRecord.Builder recordBuilder);

    ByteBuffer allocate(int size, BufferPoolRecord.Builder recordBuilder);

    void recycle(ByteBuffer theBuf);

    long capacity();

    long size();

    int getSharedOptsCount();

    int getChunkSize();
}
