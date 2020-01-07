/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * @author ActionTech
 * @CreateTime 2014-8-21
 */
public abstract class FileBase extends FileChannel {

    @Override
    public synchronized int read(ByteBuffer dst, long position) throws IOException {
        long oldPos = position();
        position(position);
        int len = read(dst);
        position(oldPos);
        return len;
    }

    @Override
    public long read(ByteBuffer[] dst, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int write(ByteBuffer src, long position) throws IOException {
        long oldPos = position();
        position(position);
        int len = write(src);
        position(oldPos);
        return len;
    }

    @Override
    public long write(ByteBuffer[] src, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException();
    }
}
