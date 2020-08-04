/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.impl.aio;

import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.WriteOutTask;
import com.actiontech.dble.net.connection.AbstractConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class AIOSocketWR extends SocketWR {
    protected static final Logger LOGGER = LoggerFactory.getLogger(SocketWR.class);
    private static final AIOReadHandler AIO_READ_HANDLER = new AIOReadHandler();
    private static final AIOWriteHandler AIO_WRITE_HANDLER = new AIOWriteHandler();
    private AsynchronousSocketChannel channel;
    protected AbstractConnection con;
    protected final AtomicBoolean writing = new AtomicBoolean(false);
    private ConcurrentLinkedQueue<WriteOutTask> writeQueue;

    private volatile WriteOutTask leftoverWriteTask;

    public AIOSocketWR() {

    }

    public void initFromConnection(AbstractConnection connection) {
        this.con = connection;
        this.channel = (AsynchronousSocketChannel) connection.getChannel();
        this.writeQueue = connection.getWriteQueue();
    }

    @Override
    public void asyncRead() {
        ByteBuffer theBuffer = con.findReadBuffer();
        if (theBuffer.hasRemaining()) {
            channel.read(theBuffer, this, AIO_READ_HANDLER);
        } else {
            throw new IllegalArgumentException("full buffer to read ");
        }

    }

    private void asyncWrite(final ByteBuffer buffer) {
        buffer.flip();
        this.channel.write(buffer, this, AIO_WRITE_HANDLER);

    }

    /**
     * return true ,means no more data
     *
     * @return
     */
    private boolean write0() {
        if (!writing.compareAndSet(false, true)) {
            return false;
        }
        ByteBuffer theBuffer = leftoverWriteTask == null ? null : leftoverWriteTask.getBuffer();
        if (theBuffer == null || !theBuffer.hasRemaining()) {
            if (theBuffer != null) {
                con.recycle(theBuffer);
                leftoverWriteTask = null;
            }
            // poll again
            WriteOutTask task = writeQueue.poll();
            // more data
            if (task != null) {
                ByteBuffer buffer = task.getBuffer();
                boolean quitFlag = task.closeFlag();
                if (buffer.limit() == 0) {
                    con.recycle(buffer);
                    leftoverWriteTask = null;
                    con.close("quit cmd");
                    writing.set(false);
                    return true;
                } else {
                    leftoverWriteTask = task;
                    asyncWrite(buffer);
                    if (quitFlag) {
                        con.recycle(buffer);
                        con.close(con.getCloseReason());
                        return true;
                    }
                    return false;
                }
            } else {
                // no buffer
                writing.set(false);
                return true;
            }
        } else {
            boolean quitFlag = leftoverWriteTask == null ? false : leftoverWriteTask.closeFlag();
            theBuffer.compact();
            asyncWrite(theBuffer);
            if (quitFlag) {
                con.recycle(theBuffer);
                con.close(con.getCloseReason());
                return true;
            }
            return false;
        }

    }

    protected void onWriteFinished(int result) {
        con.writeStatistics(result);
        boolean noMoreData = this.write0();
        if (noMoreData) {
            this.doNextWriteCheck();
        }

    }

    public void doNextWriteCheck() {

        boolean noMoreData = false;
        noMoreData = this.write0();
        if (noMoreData && !con.getWriteQueue().isEmpty()) {
            this.write0();
        }
    }

    public boolean registerWrite(ByteBuffer buffer) {
        leftoverWriteTask = new WriteOutTask(buffer, false);
        buffer.flip();
        try {
            write0();
        } catch (Exception e) {
            //SLB no exception in AIO
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("caught err:", e);
            }
            con.close("err:" + e);
            return false;
        }
        return true;
    }

    @Override
    public void disableRead() {

    }

    @Override
    public void enableRead() {

    }
}


