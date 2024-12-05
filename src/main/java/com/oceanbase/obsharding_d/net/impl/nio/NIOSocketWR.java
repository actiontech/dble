/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.impl.nio;

import com.oceanbase.obsharding_d.net.SocketWR;
import com.oceanbase.obsharding_d.net.WriteOutTask;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.service.CloseType;
import com.oceanbase.obsharding_d.net.service.ServiceTaskFactory;
import com.oceanbase.obsharding_d.singleton.FlowController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class NIOSocketWR extends SocketWR {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOSocketWR.class);
    public static final int NOT_USED = -1;
    private volatile SelectionKey processKey;
    private static final int OP_NOT_READ = ~SelectionKey.OP_READ;
    private static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;
    private AbstractConnection con;
    private SocketChannel channel;
    private final AtomicLong writing = new AtomicLong(NOT_USED);
    private ConcurrentLinkedQueue<WriteOutTask> writeQueue;

    private volatile WriteOutTask leftoverWriteTask;
    private volatile boolean writeDataErr = false;

    private volatile boolean disableReadForever = false;

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
                clearWriteQueue();
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
            //when write errored,the flow control doesn't work
            clearWriteQueue();
            con.getWritingSize().set(0);
            if (Objects.equals(e.getMessage(), "Broken pipe") || Objects.equals(e.getMessage(), "Connection reset by peer") ||
                    e instanceof ClosedChannelException) {
                // target problem,
                //ignore this exception,will close by read side.
                LOGGER.warn("Connection was closed while write. Detail reason:{}. {}.", e.toString(), con.getService());
            } else {
                //self problem.
                LOGGER.info("con {} write err:", con.getService(), e);
                con.pushServiceTask(ServiceTaskFactory.getInstance(con.getService()).createForForceClose(e.getMessage(), CloseType.WRITE));
            }

        } catch (Exception e) {
            writeDataErr = true;
            //connection was closed,nio channel may be closed
            if (con.isClosed()) {
                LOGGER.info("connection was closed while write, con is {}, reason is {}", con.getService(), e);
            } else {
                LOGGER.warn("con {} write err:", con.getService(), e);
            }
            clearWriteQueue();
            con.getWritingSize().set(0);
            con.pushServiceTask(ServiceTaskFactory.getInstance(con.getService()).createForForceClose(e.getMessage(), CloseType.WRITE));

        } finally {
            if (writeDataErr) {
                clearWriteQueue();
                con.getWritingSize().set(0);
                if (FlowController.isEnableFlowControl() && con.isFrontWriteFlowControlled()) {
                    con.stopFlowControl(0);
                }
                if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
                    disableWrite();
                }
            }
            /*
            there are exists two invocation used to mark writing flag to unused.(writing.compareAndSet(threadId, NOT_USED);)
            this first one is preserved to make ensure forward compatibility.
            if the first one called, the second one will doesn't take any effect, even if the writing flag is marked used by other thread.
             */
            writing.compareAndSet(threadId, NOT_USED);

        }

    }

    protected void clearWriteQueue() {
        WriteOutTask task;
        while (((task = writeQueue.poll()) != null)) {
            con.recycle(task.getBuffer());
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
        if (FlowController.isEnableFlowControl()) {
            con.getWritingSize().addAndGet(buffer.position());
        }
        buffer.flip();
        try {
            write0();
        } catch (IOException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("caught err:", e);
            }
            con.close("connection was closed before first register.may be just a heartbeat from SLB/LVS. detail: [" + e.toString() + "]");
            return false;
        } finally {
            writing.compareAndSet(threadId, NOT_USED);
        }
        return true;
    }

    @Override
    public void disableRead() {
        try {
            SelectionKey key = this.processKey;
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() & OP_NOT_READ);
            }
        } catch (CancelledKeyException e) {
            //ignore error
        }
    }

    @Override
    public void disableReadForever() {
        this.disableReadForever = true;
        disableRead();
    }

    @Override
    public void enableRead() {
        if (disableReadForever) {
            return;
        }
        try {
            SelectionKey key = this.processKey;
            key.interestOps(key.interestOps() | SelectionKey.OP_READ);
            processKey.selector().wakeup();
        } catch (CancelledKeyException e) {
            //ignore error
        }
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
        int written = 0;
        boolean quitFlag = false;
        if (leftoverWriteTask != null) {
            ByteBuffer buffer = leftoverWriteTask.getBuffer();
            while (buffer.hasRemaining()) {
                quitFlag = leftoverWriteTask.closeFlag();
                try {
                    written = channel.write(buffer);
                    if (written > 0) {
                        con.writeStatistics(written);
                        if (FlowController.isEnableFlowControl()) {
                            int currentWritingSize = con.getWritingSize().addAndGet(-written);
                            con.stopFlowControl(currentWritingSize);
                        }
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
        ByteBuffer buffer;
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
                        if (FlowController.isEnableFlowControl()) {
                            int currentWritingSize = con.getWritingSize().addAndGet(-written);
                            con.stopFlowControl(currentWritingSize);
                        }
                    } else {
                        break;
                    }
                }
            } catch (Throwable e) {
                con.recycle(buffer);
                throw e;
            }

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
        if (con.isClosed()) {
            throw new IOException("read from closed channel cause error");
        }
        try {
            ByteBuffer theBuffer = con.findReadBuffer();
            int got = channel.read(theBuffer);
            con.onReadData(got);
        } finally {
            //prevent  asyncClose and read operation happened Concurrently.
            if (con.isClosed() && con.getBottomReadBuffer() != null) {
                con.recycleReadBuffer();
            }
        }
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

    @Override
    public boolean canNotWrite() {
        return writeDataErr;
    }
}
