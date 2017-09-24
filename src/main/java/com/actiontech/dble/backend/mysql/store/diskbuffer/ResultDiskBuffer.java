/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.diskbuffer;

import com.actiontech.dble.backend.mysql.store.FileStore;
import com.actiontech.dble.backend.mysql.store.result.ResultExternal;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.util.exception.NotSupportException;

import java.nio.ByteBuffer;

/**
 * a buffer used to store large amount of data on disk or virtual memory mapped
 * on disk
 *
 * @author ActionTech
 */
public abstract class ResultDiskBuffer implements ResultExternal {
    protected final int columnCount;
    protected final BufferPool pool;

    protected ByteBuffer writeBuffer;
    protected FileStore file;
    protected int rowCount = 0;

    public ResultDiskBuffer(BufferPool pool, int columnCount) {
        this.pool = pool;
        this.columnCount = columnCount;
        this.writeBuffer = pool.allocate();
        this.file = new FileStore("nioMapped:Memory", "rw");
    }

    @Override
    public void done() {
        this.file.seek(0);
    }

    @Override
    public int removeRow(RowDataPacket row) {
        throw new NotSupportException("unsupportted remove row");
    }

    @Override
    public boolean contains(RowDataPacket row) {
        throw new NotSupportException("unsupportted contains");
    }

    @Override
    public int addRow(RowDataPacket row) {
        throw new NotSupportException("unsupportted addRow");
    }

    @Override
    public ResultExternal createShallowCopy() {
        throw new NotSupportException("unsupportted createShallowCopy");
    }

    @Override
    public void close() {
        if (file != null)
            file.closeAndDeleteSilently();
        file = null;
        pool.recycle(writeBuffer);
    }

    protected ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
        int offset = 0;
        int len = src.length;
        int remaining = buffer.remaining();
        while (len > 0) {
            if (remaining >= len) {
                buffer.put(src, offset, len);
                break;
            } else {
                buffer.put(src, offset, remaining);
                buffer.flip();
                file.write(buffer);
                buffer.clear();
                offset += remaining;
                len -= remaining;
                remaining = buffer.remaining();
                continue;
            }
        }
        return buffer;
    }

    static class TapeItem {

        RowDataPacket row;
        ResultDiskTape tape;

        TapeItem(RowDataPacket row, ResultDiskTape tape) {
            this.row = row;
            this.tape = tape;
        }
    }

    /**
     * Represents a virtual disk tape for the merge sort algorithm. Each virtual
     * disk tape is a region of the temp file.
     */
    static class ResultDiskTape {

        BufferPool pool;
        FileStore file;
        int fieldCount;
        long filePos;
        long start;
        long end;
        long pos;
        int readBufferOffset;
        ByteBuffer readBuffer;

        ResultDiskTape(BufferPool pool, FileStore file, int fieldCount) {
            this.pool = pool;
            this.file = file;
            this.fieldCount = fieldCount;
            this.readBuffer = pool.allocate();
        }

        public boolean isEnd() {
            return isReadAll();
        }

        public RowDataPacket nextRow() {
            if (isReadAll())
                return null;
            byte[] row = getRow();
            RowDataPacket currentRow = new RowDataPacket(fieldCount);
            currentRow.read(row);
            return currentRow;
        }

        private boolean isReadAll() {
            return this.end == this.pos;
        }

        private void readIntoBuffer() {
            file.seek(filePos);
            filePos += file.read(readBuffer, end);
        }

        private byte[] getRow() {
            int offset = readBufferOffset, length = 0, position = readBuffer.position();
            length = getPacketLength(readBuffer, offset);
            while (length == -1 || position < offset + length) {
                if (!readBuffer.hasRemaining()) {
                    checkReadBuffer(offset);
                }
                // read new data to buffer
                readIntoBuffer();
                // get new offset for buffer compact
                offset = readBufferOffset;
                position = readBuffer.position();
                if (length == -1) {
                    length = getPacketLength(readBuffer, offset);
                }
            }

            readBuffer.position(offset);
            byte[] data = new byte[length];
            readBuffer.get(data, 0, length);
            offset += length;
            pos += length;
            if (position == offset) {
                if (readBufferOffset != 0) {
                    readBufferOffset = 0;
                }
                readBuffer.clear();
                readIntoBuffer();
            } else {
                readBufferOffset = offset;
                readBuffer.position(position);
            }
            return data;
        }

        private int getPacketLength(ByteBuffer buffer, int offset) {
            if (buffer.position() < offset + 4) {
                return -1;
            } else {
                int length = buffer.get(offset) & 0xff;
                length |= (buffer.get(++offset) & 0xff) << 8;
                length |= (buffer.get(++offset) & 0xff) << 16;
                return length + 4;
            }
        }

        private void checkReadBuffer(int offset) {
            // if offset is 0,then expend buffer; else set offset to 0,compact
            // buffer
            if (offset == 0) {
                if (readBuffer.capacity() >= Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Packet size over the limit.");
                }
                int size = readBuffer.capacity() > (Integer.MAX_VALUE >> 1) ? Integer.MAX_VALUE : readBuffer.capacity() << 1;
                ByteBuffer newBuffer = ByteBuffer.allocate(size);
                readBuffer.position(offset);
                newBuffer.put(readBuffer);
                pool.recycle(readBuffer);
                readBuffer = newBuffer;
            } else {
                readBuffer.position(offset);
                readBuffer.compact();
                readBufferOffset = 0;
            }
        }
    }

}
