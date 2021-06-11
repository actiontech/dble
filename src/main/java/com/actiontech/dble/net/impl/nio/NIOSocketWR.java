/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.impl.nio;

import com.actiontech.dble.config.FlowControllerConfig;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.WriteOutTask;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.service.ServiceTaskFactory;
import com.actiontech.dble.singleton.WriteQueueFlowController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class NIOSocketWR extends SocketWR {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOSocketWR.class);
    public static final int NOT_USED = -1;
    private SelectionKey processKey;
    private static final int OP_NOT_READ = ~SelectionKey.OP_READ;
    private static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;
    private AbstractConnection con;
    private SocketChannel channel;
    private final AtomicLong writing = new AtomicLong(NOT_USED);
    private ConcurrentLinkedQueue<WriteOutTask> writeQueue;

    private volatile WriteOutTask leftoverWriteTask;
    private volatile boolean writeDataErr = false;

    public void initFromConnection(AbstractConnection connection) {
        this.con = connection;
        this.channel = (SocketChannel) connection.getChannel();
        this.writeQueue = connection.getWriteQueue();
    }

    public void register(Selector selector) throws IOException {
        try {
            processKey = channel.register(selector, SelectionKey.OP_READ, con);
        } finally {
            if (con.isClosed()) {
                clearSelectionKey();
            }
        }
    }

    public void doNextWriteCheck() {
        final long threadId = Thread.currentThread().getId();
        if (!writing.compareAndSet(NOT_USED, threadId)) {
            return;
        }

        try {
            if (writeDataErr) {
                this.writeQueue.clear();
                return;
            }
            boolean noMoreData = write0();
            writing.compareAndSet(threadId, NOT_USED);
            if (noMoreData && writeQueue.isEmpty()) {
                if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
                    disableWrite();
                }
            } else {
                if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
                    enableWrite(false);
                }
            }

        } catch (IOException e) {
            writeDataErr = true;
            if (Objects.equals(e.getMessage(), "Broken pipe") || e instanceof ClosedChannelException) {
                // target problem,
                //ignore this exception,will close by read side.
                LOGGER.debug("Connection was closed while read. Detail reason:{}. {}.", e.toString(), con.getService());
            } else {
                //self problem.
                LOGGER.info("con {} write err:", con.getService(), e);
                con.pushServiceTask(ServiceTaskFactory.getInstance(con.getService()).createForForceClose(e.getMessage()));
            }

        } catch (Exception e) {
            writeDataErr = true;
            LOGGER.info("con {} write err:", con.getService(), e);
            con.pushServiceTask(ServiceTaskFactory.getInstance(con.getService()).createForForceClose(e.getMessage()));

        } finally {
            if (writeDataErr) {
                this.writeQueue.clear();
            }
            writing.compareAndSet(threadId, NOT_USED);

        }

    }

    public boolean registerWrite(ByteBuffer buffer) {
        final long threadId = Thread.currentThread().getId();
        while (!writing.compareAndSet(NOT_USED, threadId)) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        leftoverWriteTask = new WriteOutTask(buffer, false);
        buffer.flip();
        try {
            write0();
            writing.compareAndSet(threadId, NOT_USED);
        } catch (IOException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("caught err:", e);
            }
            LOGGER.info("GET IOException when registerWrite,may be just a heartbeat from SLB/LVS :" + e.getMessage());
            con.close("err:" + e);
            return false;
        }
        return true;
    }

    @Override
    public void disableRead() {
        SelectionKey key = this.processKey;
        key.interestOps(key.interestOps() & OP_NOT_READ);
    }

    @Override
    public void enableRead() {
        SelectionKey key = this.processKey;
        key.interestOps(key.interestOps() | SelectionKey.OP_READ);
        processKey.selector().wakeup();
    }

    @Override
    public boolean isWriteComplete() {
        if (writeDataErr) {
            return true;
        }
        if (!writeQueue.isEmpty()) {
            return false;
        }
        final long threadId = Thread.currentThread().getId();
        while (!writing.compareAndSet(NOT_USED, threadId)) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            ByteBuffer buffer = leftoverWriteTask == null ? null : leftoverWriteTask.getBuffer();
            if (buffer != null && buffer.hasRemaining()) {
                return false;
            }
            return writeQueue.isEmpty();
        } finally {
            writing.set(NOT_USED);
        }

    }

    private boolean write0() throws IOException {

        int flowControlCount = -1;
        int written = 0;
        boolean quitFlag = false;
        ByteBuffer buffer = leftoverWriteTask == null ? null : leftoverWriteTask.getBuffer();
        if (buffer != null) {
            while (buffer.hasRemaining()) {
                quitFlag = leftoverWriteTask == null ? false : leftoverWriteTask.closeFlag();
                try {
                    written = channel.write(buffer);
                    if (written > 0) {
                        con.writeStatistics(written);
                    } else {
                        break;
                    }
                } catch (Throwable e) {
                    leftoverWriteTask = null;
                    con.recycle(buffer);
                    if (!quitFlag) {
                        throw e;
                    } else {
                        con.close(con.getCloseReason());
                        LOGGER.info("writeDirectly quit error and ignore ");
                        return true;
                    }
                }
            }

            flowControlCount = checkFlowControl(flowControlCount);

            if (quitFlag) {
                con.recycle(buffer);
                con.close(con.getCloseReason());
                return true;
            }

            if (buffer.hasRemaining()) {
                return false;
            } else {
                leftoverWriteTask = null;
                con.recycle(buffer);
            }
        }
        WriteOutTask task;
        while ((task = writeQueue.poll()) != null) {
            quitFlag = task.closeFlag();
            buffer = task.getBuffer();
            if (buffer.limit() == 0) {
                con.recycle(buffer);
                con.close("quit send");
                return true;
            }

            buffer.flip();
            try {
                while (buffer.hasRemaining()) {
                    written = channel.write(buffer);
                    if (written > 0) {
                        con.writeStatistics(written);
                    } else {
                        break;
                    }
                }
            } catch (Throwable e) {
                con.recycle(buffer);
                throw e;
            }

            flowControlCount = checkFlowControl(flowControlCount);

            if (quitFlag) {
                con.recycle(buffer);
                con.close(con.getCloseReason());
                return true;
            }

            if (buffer.hasRemaining()) {
                leftoverWriteTask = task;
                return false;
            } else {
                con.recycle(buffer);
            }
        }
        return true;
    }

    private int checkFlowControl(int flowControlCount) {
        FlowControllerConfig config = WriteQueueFlowController.getFlowCotrollerConfig();
        if (con.isFlowControlled()) {
            if (!config.isEnableFlowControl()) {
                con.stopFlowControl();
                return -1;
            } else if ((flowControlCount != -1) &&
                    (flowControlCount <= config.getEnd())) {
                int currentSize = this.writeQueue.size();
                if (currentSize <= config.getEnd()) {
                    con.stopFlowControl();
                    return -1;
                } else {
                    return currentSize;
                }
            } else if (flowControlCount == -1) {
                int currentSize = this.writeQueue.size();
                if (currentSize <= config.getEnd()) {
                    con.stopFlowControl();
                    return -1;
                } else {
                    return currentSize;
                }
            } else {
                return --flowControlCount;
            }
        } else {
            return -1;
        }
    }


    private void disableWrite() {
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() & OP_NOT_WRITE);
        } catch (Exception e) {
            LOGGER.info("can't disable writeDirectly " + e + " con " + con);
        }

    }

    private void enableWrite(boolean wakeup) {
        boolean needWakeup = false;
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            needWakeup = true;
        } catch (Exception e) {
            LOGGER.info("can't enable writeDirectly " + e);

        }
        if (needWakeup && wakeup) {
            processKey.selector().wakeup();
        }
    }

    private void clearSelectionKey() {
        try {
            SelectionKey key = this.processKey;
            if (key != null && key.isValid()) {
                key.attach(null);
                key.cancel();
            }
        } catch (Exception e) {
            LOGGER.info("clear selector keys err:" + e);
        }
    }

    @Override
    public void asyncRead() throws IOException {
        ByteBuffer theBuffer = con.findReadBuffer();
        int got = channel.read(theBuffer);
        con.onReadData(got);
    }


    @Override
    public void shutdownInput() throws IOException {
        channel.shutdownInput();
    }

    @Override
    public void closeSocket() throws IOException {
        clearSelectionKey();
        channel.close();
    }
}
