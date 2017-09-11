/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.fs;

import com.actiontech.dble.DbleServer;
import org.apache.log4j.Logger;
import sun.nio.ch.DirectBuffer;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * Uses memory mapped files. The file size is limited to 2 GB.
 */
class FileNioMapped extends FileBase {

    private static Logger logger = Logger.getLogger(FileNioMapped.class);
    private static final long GC_TIMEOUT_MS = 10000;
    private final String name;
    private final MapMode mode;
    private RandomAccessFile file;
    private MappedByteBuffer mapped;
    private long fileLength;
    /**
     * The position within the file. Can't use the position of the mapped buffer
     * because it doesn't support seeking past the end of the file.
     */
    private int pos;

    FileNioMapped(String fileName, String mode) throws IOException {
        if ("r".equals(mode)) {
            this.mode = MapMode.READ_ONLY;
        } else {
            this.mode = MapMode.READ_WRITE;
        }
        this.name = fileName;
        file = new RandomAccessFile(fileName, mode);
        try {
            reMap();
        } catch (IOException e) {
            if (file != null) {
                file.close();
                file = null;
            }
            throw e;
        }
    }

    private void unMap() throws IOException {
        if (mapped == null) {
            return;
        }
        // first write all data
        // mapped.force();

        // need to dispose old direct buffer, see bug
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038

        boolean useSystemGc = true;
        try {
            Method cleanerMethod = mapped.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(mapped);
            if (cleaner != null) {
                Method clearMethod = cleaner.getClass().getMethod("clean");
                clearMethod.invoke(cleaner);
            }
            useSystemGc = false;
        } catch (Throwable e) {
            logger.warn("unmap byteBuffer error", e);
            // useSystemGc is already true
        } finally {
            mapped = null;
        }

        if (useSystemGc) {
            WeakReference<MappedByteBuffer> bufferWeakRef = new WeakReference<>(mapped);
            mapped = null;
            long start = System.currentTimeMillis();
            while (bufferWeakRef.get() != null) {
                if (System.currentTimeMillis() - start > GC_TIMEOUT_MS) {
                    throw new IOException(
                            "Timeout (" + GC_TIMEOUT_MS + " ms) reached while trying to GC mapped buffer");
                }
                System.gc();
                Thread.yield();
            }
        }
    }

    /**
     * Re-map byte buffer into memory, called when file size has changed or file
     * was created.
     */
    private void reMap() throws IOException {
        int oldPos = 0;
        if (mapped != null) {
            oldPos = pos;
            unMap();
        }
        fileLength = file.length();
        if (fileLength == 0) {
            // fileLength = 1024*1024* 1024;
            fileLength = DbleServer.getInstance().getConfig().getSystem().getMappedFileSize();
        }
        checkFileSizeLimit(fileLength);
        // maps new MappedByteBuffer; the old one is disposed during GC
        mapped = file.getChannel().map(mode, 0, fileLength);
        int limit = mapped.limit();
        int capacity = mapped.capacity();
        if (limit < fileLength || capacity < fileLength) {
            throw new IOException("Unable to map: length=" + limit + " capacity=" + capacity + " length=" + fileLength);
        }
        boolean nioLoadMapped = false;
        if (nioLoadMapped) {
            mapped.load();
        }
        this.pos = Math.min(oldPos, (int) fileLength);
    }

    private static void checkFileSizeLimit(long length) throws IOException {
        if (length > Integer.MAX_VALUE) {
            throw new IOException("File over 2GB is not supported yet when using this file system");
        }
    }

    @Override
    public synchronized void implCloseChannel() throws IOException {
        if (file != null) {
            unMap();
            file.close();
            file = null;
        }
    }

    @Override
    public synchronized long position() {
        return pos;
    }

    @Override
    public synchronized FileChannel position(long newPosition) throws IOException {
        checkFileSizeLimit(newPosition);
        this.pos = (int) newPosition;
        return this;
    }

    @Override
    public String toString() {
        return "nioMapped:" + name;
    }

    @Override
    public synchronized long size() throws IOException {
        return fileLength;
    }

    @Override
    public synchronized int read(ByteBuffer dst) throws IOException {
        try {
            int len = dst.remaining();
            if (len == 0) {
                return 0;
            }
            len = (int) Math.min(len, fileLength - pos);
            if (len <= 0) {
                return -1;
            }
            mapped.position(pos);
            if (dst instanceof DirectBuffer) {
                byte[] temp = new byte[len];
                mapped.get(temp, 0, len);
                dst.put(temp);
            } else {
                mapped.get(dst.array(), dst.arrayOffset() + dst.position(), len);
                dst.position(dst.position() + len);
            }
            pos += len;
            return len;
        } catch (IllegalArgumentException | BufferUnderflowException e) {
            EOFException e2 = new EOFException("EOF");
            e2.initCause(e);
            throw e2;
        }
    }

    @Override
    public synchronized FileChannel truncate(long newLength) throws IOException {
        if (newLength < size()) {
            setFileLength(newLength);
        }
        return this;
    }

    public synchronized void setFileLength(long newLength) throws IOException {
        checkFileSizeLimit(newLength);
        final int oldPos = pos;
        unMap();
        for (int i = 0; ; i++) {
            try {
                file.setLength(newLength);
                break;
            } catch (IOException e) {
                if (i > 16 || !e.toString().contains("user-mapped section open")) {
                    throw e;
                }
            }
            System.gc();
        }
        reMap();
        pos = (int) Math.min(newLength, oldPos);
    }

    @Override
    public synchronized void force(boolean metaData) throws IOException {
        mapped.force();
        file.getFD().sync();
    }

    /**
     * don't expand
     */
    @Override
    public synchronized int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        if (mapped.capacity() < pos + len) {
            int offset = src.position();
            int length = mapped.capacity() - pos;
            if (src instanceof DirectBuffer) {
                byte[] temp = new byte[length];
                src.get(temp, 0, length);
                mapped.put(temp, 0, length);
                temp = null;
            } else {
                mapped.put(src.array(), offset, length);
            }
            src.position(offset + length);
            pos += length;
            return length;
        } else {
            mapped.put(src);
            pos += len;
            return len;
        }
    }

    @Override
    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return file.getChannel().tryLock(position, size, shared);
    }
}
