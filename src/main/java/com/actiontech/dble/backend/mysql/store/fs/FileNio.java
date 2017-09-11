/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.fs;

import com.actiontech.dble.DbleServer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;

/**
 * File which uses NIO FileChannel.
 */
class FileNio extends FileBase {

    private final String name;
    private final FileChannel channel;
    private long fileLength;
    private long pos;

    FileNio(String fileName, String mode) throws IOException {
        this.name = fileName;
        RandomAccessFile file = new RandomAccessFile(fileName, mode);
        this.channel = file.getChannel();
        this.fileLength = DbleServer.getInstance().getConfig().getSystem().getMappedFileSize();
        this.pos = 0;
    }

    @Override
    public synchronized void implCloseChannel() throws IOException {
        channel.close();
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public synchronized FileChannel position(long newPosition) throws IOException {
        channel.position(newPosition);
        this.pos = (int) newPosition;
        return this;
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public synchronized int read(ByteBuffer dst) throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        len = (int) Math.min(len, fileLength - pos);
        if (len <= 0) {
            return -1;
        }
        int limit = dst.limit();
        dst.limit(dst.position() + len);
        channel.read(dst);
        pos += len;
        dst.limit(limit);
        return len;
    }

    @Override
    public synchronized int read(ByteBuffer dst, long position) throws IOException {
        return channel.read(dst, position);
    }

    @Override
    public synchronized int write(ByteBuffer src, long position) throws IOException {
        return channel.write(src, position);
    }

    @Override
    public synchronized int write(ByteBuffer src) throws IOException {
        try {
            int len;
            if (fileLength < pos + src.remaining()) {
                int length = (int) (fileLength - pos);
                int limit = src.limit();
                src.limit(length);
                len = channel.write(src);
                src.limit(limit);
                pos += len;
                return len;
            } else {
                len = channel.write(src);
                pos += len;
                return len;
            }
        } catch (NonWritableChannelException e) {
            throw new IOException("read only");
        }
    }

    @Override
    public synchronized FileChannel truncate(long newLength) throws IOException {
        try {
            long size = channel.size();
            if (newLength < size) {
                long position = channel.position();
                channel.truncate(newLength);
                long newPos = channel.position();
                if (position < newLength) {
                    // position should stay
                    // in theory, this should not be needed
                    if (newPos != position) {
                        channel.position(position);
                        this.pos = position;
                    }
                } else if (newPos > newLength) {
                    // looks like a bug in this FileChannel implementation, as
                    // the documentation says the position needs to be changed
                    channel.position(newLength);
                    this.pos = newLength;
                }
            }
            return this;
        } catch (NonWritableChannelException e) {
            throw new IOException("read only");
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return channel.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return "nio:" + name;
    }

}
