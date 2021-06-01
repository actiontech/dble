package com.actiontech.dble.net.connection;

import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandler;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;
import com.actiontech.dble.btrace.provider.IODelayProvider;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.WriteOutTask;
import com.actiontech.dble.net.service.*;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.statistic.sql.StatisticListener;
import com.actiontech.dble.util.CompressUtil;
import com.actiontech.dble.util.DebugUtil;
import com.actiontech.dble.util.TimeUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NetworkChannel;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by szf on 2020/6/15.
 */
public abstract class AbstractConnection implements Connection {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnection.class);

    protected final NetworkChannel channel;
    protected final SocketWR socketWR;
    protected final AtomicBoolean isClosed;

    protected long id;
    protected String host;
    protected int localPort;
    protected int port;

    private volatile ProtoHandler proto;
    private volatile boolean isSupportCompress;
    private volatile AbstractService service;
    protected volatile IOProcessor processor;
    protected volatile String closeReason;
    protected volatile ByteBuffer readBuffer;
    private volatile boolean flowControlled;
    protected int readBufferChunk;
    protected final long startupTime;
    protected volatile long lastReadTime;
    protected volatile long lastWriteTime;
    protected long netInBytes;
    protected long netOutBytes;
    protected long lastLargeMessageTime;
    private int extraPartOfBigPacketCount = 0;


    protected Set<String> graceClosedReasons = Sets.newConcurrentHashSet();
    protected AtomicBoolean doingGracefulClose = new AtomicBoolean(false);

    protected final ConcurrentLinkedQueue<WriteOutTask> writeQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue = new ConcurrentLinkedQueue<>();

    public AbstractConnection(NetworkChannel channel, SocketWR socketWR) {
        this.channel = channel;
        this.socketWR = socketWR;
        this.isClosed = new AtomicBoolean(false);
        this.startupTime = TimeUtil.currentTimeMillis();
        this.lastReadTime = startupTime;
        this.lastWriteTime = startupTime;
        this.proto = new MySQLProtoHandlerImpl();
    }

    public void onReadData(int got) throws IOException {
        if (isClosed.get()) {
            return;
        }

        lastReadTime = TimeUtil.currentTimeMillis();
        if (got == -1) {
            if (doingGracefulClose.get()) {
                pushInnerServiceTask(ServiceTaskFactory.getInstance(service).createForGracefulClose(graceClosedReasons));
            } else {
                pushInnerServiceTask(ServiceTaskFactory.getInstance(service).createForGracefulClose("stream closed by peer"));
            }
            return;
        } else if (got == 0 && !this.channel.isOpen()) {
            pushInnerServiceTask(ServiceTaskFactory.getInstance(service).createForGracefulClose("stream is closed when reading zero byte"));
            return;
        } else {
            netInBytes += got;
        }

        handle(readBuffer);
    }


    @Override
    public synchronized void closeGracefully(@Nonnull String reason) {
        if (doingGracefulClose.compareAndSet(false, true)) {
            synchronized (this) {
                if (isClosed()) {
                    LOGGER.info("connection  gracefully ignored. for reason {}", reason);
                    return;
                }
                graceClosedReasons.add(reason);
                //shutdownInput will trigger  a read event of IO reactor and return -1.
                try {
                    socketWR.shutdownInput();
                } catch (IOException e) {

                    if (isClosed()) {
                        LOGGER.error("close gracefully cause error.ignored reason is {}", reason, e);
                    } else {
                        LOGGER.error("close gracefully cause error.reason is {}", reason, e);
                        pushInnerServiceTask(ServiceTaskFactory.getInstance(service).createForForceClose(reason));
                    }
                }


            }
        }
    }

    @Override
    public synchronized void closeImmediately(final String reason) {
        closeImmediatelyInner(reason);
    }

    private void closeImmediatelyInner(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            if (service instanceof BusinessService)
                ((BusinessService) service).transactionsCountInTx();
            Optional.ofNullable(StatisticListener.getInstance().getRecorder(service)).ifPresent(r -> r.onTxEndByExit());
            StatisticListener.getInstance().remove(service);
            closeSocket();
            LOGGER.info("connection id close for reason " + reason + " with connection " + toString());
            if (processor != null) {
                processor.removeConnection(this);
            }

            this.cleanup();

            // ignore null information
            if (Strings.isNullOrEmpty(reason)) {
                return;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("close connection,reason:" + reason + " ," + this);
            }
            if (reason.contains("connection,reason:java.net.ConnectException")) {
                throw new RuntimeException(reason);
            }
        } else {
            // make sure buffer recycle again, avoid buffer leak
            this.cleanup();
        }
    }


    public boolean pushInnerServiceTask(InnerServiceTask innerServiceTask) {
        IODelayProvider.beforePushInnerServiceTask(innerServiceTask, service);
        if (service == null) {
            if (isClosed()) {
                LOGGER.info("can't delay process the service task ,connection is closed already. just ignored.  {}", innerServiceTask.getType());
                return false;
            } else {
                LOGGER.info("can't delay process the service task ,Maybe the connection is not ready yet. try process immediately {}", innerServiceTask.getType());
                DebugUtil.printLocation();
            }

            switch (innerServiceTask.getType()) {
                case CLOSE:
                    this.closeImmediately(Joiner.on(',').join(((CloseServiceTask) innerServiceTask).getReasons()));
                    break;
                default:
                    LOGGER.error("illegal service task. {}", innerServiceTask);
                    return false;
            }
            return true;
        }
        service.handle(innerServiceTask);
        return true;
    }




    private void handle(ByteBuffer dataBuffer) {
        boolean hasRemaining = true;
        int offset = 0;
        while (hasRemaining) {
            ProtoHandlerResult result = proto.handle(dataBuffer, offset, isSupportCompress);
            switch (result.getCode()) {
                case PART_OF_BIG_PACKET:

                    extraPartOfBigPacketCount++;
                    if (!result.isHasMorePacket()) {
                        readReachEnd();
                        dataBuffer.clear();
                    }

                    break;
                case COMPLETE_PACKET:
                    processPacketData(result);
                    if (!result.isHasMorePacket()) {
                        readReachEnd();
                        dataBuffer.clear();
                    }
                    break;
                case BUFFER_PACKET_UNCOMPLETE:
                    compactReadBuffer(dataBuffer, result.getOffset());
                    break;
                case BUFFER_NOT_BIG_ENOUGH:
                    ensureFreeSpaceOfReadBuffer(dataBuffer, result.getOffset(), result.getPacketLength());
                    break;
                default:
                    throw new RuntimeException("unknown error when read data");
            }

            hasRemaining = result.isHasMorePacket();
            if (hasRemaining) {
                offset = result.getOffset();
            }
        }
    }

    private void processPacketData(ProtoHandlerResult result) {
        byte[] packetData = result.getPacketData();
        if (packetData != null) {
            int tmpCount = extraPartOfBigPacketCount;
            if (!isSupportCompress) {
                extraPartOfBigPacketCount = 0;
                service.handle(new NormalServiceTask(packetData, service, tmpCount));
            } else {
                List<byte[]> packs = CompressUtil.decompressMysqlPacket(packetData, decompressUnfinishedDataQueue);
                if (decompressUnfinishedDataQueue.isEmpty()) {
                    extraPartOfBigPacketCount = 0;
                }
                for (byte[] pack : packs) {
                    if (pack.length != 0) {
                        service.handle(new NormalServiceTask(pack, service, tmpCount));
                    }
                }
            }
        }
    }

    public void close(String reason) {
        this.closeImmediatelyInner(reason);
    }

    public void close(Exception exception) {
        LOGGER.info("get Exception close ", exception);
        this.close(exception.getMessage());
    }

    private void closeSocket() {
        if (channel != null) {
            try {
                socketWR.closeSocket();
            } catch (Exception e) {
                LOGGER.info("AbstractConnectionCloseError", e);
            }

            boolean closed = !channel.isOpen();
            if (!closed) {
                LOGGER.info("close socket of connnection failed " + this);
            }
        }
    }

    public void compactReadBuffer(ByteBuffer buffer, int offset) {
        if (buffer == null) {
            return;
        }
        buffer.limit(buffer.position());
        buffer.position(offset);
        this.readBuffer = buffer.compact();
    }

    public void ensureFreeSpaceOfReadBuffer(ByteBuffer buffer,
                                            int offset, final int pkgLength) {
        if (buffer.capacity() < pkgLength) {
            ByteBuffer newBuffer = processor.getBufferPool().allocate(pkgLength);
            lastLargeMessageTime = TimeUtil.currentTimeMillis();
            buffer.position(offset);
            newBuffer.put(buffer);
            readBuffer = newBuffer;
            recycle(buffer);
        } else {
            if (offset != 0) {
                // compact bytebuffer only
                compactReadBuffer(buffer, offset);
            } else {
                throw new RuntimeException(" not enough space");
            }
        }
    }

    public void readReachEnd() {
        // if cur buffer is temper none direct byte buffer and not
        // received large message in recent 30 seconds
        // then change to direct buffer for performance
        if (readBuffer != null && !readBuffer.isDirect() &&
                lastLargeMessageTime < lastReadTime - 30 * 1000L) {  // used temp heap
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("change to direct con read buffer ,cur temp buf size :" + readBuffer.capacity());
            }
            recycle(readBuffer);
            readBuffer = processor.getBufferPool().allocate(readBufferChunk);
        } else {
            if (readBuffer != null) {
                readBuffer.clear();
            }
        }
    }

    public void setSocketParams(boolean isFrontChannel) throws IOException {
        SystemConfig system = SystemConfig.getInstance();
        int soRcvBuf;
        int soSndBuf;
        int soNoDelay;
        if (isFrontChannel) {
            soRcvBuf = system.getFrontSocketSoRcvbuf();
            soSndBuf = system.getFrontSocketSoSndbuf();
            soNoDelay = system.getFrontSocketNoDelay();
        } else {
            soRcvBuf = system.getBackSocketSoRcvbuf();
            soSndBuf = system.getBackSocketSoSndbuf();
            soNoDelay = system.getBackSocketNoDelay();
        }

        channel.setOption(StandardSocketOptions.SO_RCVBUF, soRcvBuf);
        channel.setOption(StandardSocketOptions.SO_SNDBUF, soSndBuf);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, soNoDelay == 1);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);

        this.setReadBufferChunk(soRcvBuf);
    }

    public final void recycle(ByteBuffer buffer) {
        this.processor.getBufferPool().recycle(buffer);
    }

    public final long getId() {
        return id;
    }

    public boolean finishConnect() throws IOException {
        localPort = ((InetSocketAddress) channel.getLocalAddress()).getPort();
        return true;
    }

    public synchronized AbstractService getService() {
        return service;
    }

    public ByteBuffer allocate() {
        int size = this.processor.getBufferPool().getChunkSize();
        return this.processor.getBufferPool().allocate(size);
    }

    public ByteBuffer allocate(int size) {
        return this.processor.getBufferPool().allocate(size);
    }

    public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
        int offset = 0;
        int length = src.length;
        int remaining = buffer.remaining();
        while (length > 0) {
            if (remaining >= length) {
                buffer.put(src, offset, length);
                break;
            } else {
                buffer.put(src, offset, remaining);
                writePart(buffer);
                buffer = allocate();
                offset += remaining;
                length -= remaining;
                remaining = buffer.remaining();
            }
        }
        return buffer;
    }

    public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity, boolean writeSocketIfFull) {
        if (capacity > buffer.remaining()) {
            if (writeSocketIfFull) {
                writePart(buffer);
                return processor.getBufferPool().allocate(capacity);
            } else { // Relocate a larger buffer
                buffer.flip();
                ByteBuffer newBuf = processor.getBufferPool().allocate(capacity + buffer.limit() + 1);
                newBuf.put(buffer);
                this.recycle(buffer);
                return newBuf;
            }
        } else {
            return buffer;
        }
    }

    public void writePart(ByteBuffer buffer) {
        write(buffer);
    }

    public final boolean registerWrite(ByteBuffer buffer) {

        // if async writeDirectly finished event got lock before me ,then writing
        // flag is set false but not start a writeDirectly request
        // so we check again
        try {
            return this.socketWR.registerWrite(buffer);
        } catch (Exception e) {
            LOGGER.info("writeDirectly err:", e);
            this.pushInnerServiceTask(ServiceTaskFactory.getInstance(this.getService()).createForForceClose(e.getMessage()));
            return false;
        }
    }

    public void write(byte[] data) {
        service.writeDirectly(data);
    }

    public void write(ByteBuffer buffer) {
        if (isClosed.get()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("it will not writeDirectly because of closed " + this + " " + isClosed);
            }
            if (buffer != null) {
                recycle(buffer);
            }
            this.cleanup();
            return;
        }

        if (isSupportCompress) {
            ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer, this, compressUnfinishedDataQueue);
            writeQueue.offer(new WriteOutTask(newBuffer, false));
        } else {
            writeQueue.offer(new WriteOutTask(buffer, false));
        }

        // if async writeDirectly finished event got lock before me ,then writing
        // flag is set false but not start a writeDirectly request
        // so we check again
        this.socketWR.doNextWriteCheck();

    }

    public boolean isClosed() {
        return isClosed.get();
    }

    public ByteBuffer findReadBuffer() {
        if (readBuffer == null) {
            readBuffer = processor.getBufferPool().allocate(processor.getBufferPool().getChunkSize());
        }
        return readBuffer;
    }

    public void onConnectFailed(Throwable e) {
    }

    public synchronized void cleanup() {

        if (readBuffer != null) {
            this.recycle(readBuffer);
            this.readBuffer = null;
        }

        if (service != null) {
            service.cleanup();
        }

        if (this instanceof FrontendConnection) {
            ((FrontendConnection) this).setSkipCheck(false);
        }

        if (!decompressUnfinishedDataQueue.isEmpty()) {
            decompressUnfinishedDataQueue.clear();
        }

        if (!compressUnfinishedDataQueue.isEmpty()) {
            compressUnfinishedDataQueue.clear();
        }

        WriteOutTask task;
        while ((task = writeQueue.poll()) != null) {
            recycle(task.getBuffer());
        }
    }

    public void writeStatistics(int outBytes) {
        this.netOutBytes += outBytes;
        processor.addNetOutBytes(outBytes);
        lastWriteTime = TimeUtil.currentTimeMillis();
    }

    public void updateLastReadTime() {
        lastReadTime = TimeUtil.currentTimeMillis();
    }

    public NetworkChannel getChannel() {
        return channel;
    }

    public SocketWR getSocketWR() {
        return socketWR;
    }

    public void register() throws IOException {
        if (service instanceof AuthService) {
            ((AuthService) service).register();
        }
    }

    public ConcurrentLinkedQueue<WriteOutTask> getWriteQueue() {
        return writeQueue;
    }

    public void asyncRead() throws IOException {
        try {
            this.socketWR.asyncRead();
        } catch (ClosedChannelException e) {
            throw e;
        } catch (IOException e) {
            if (e.getMessage() == null) {
                LOGGER.info("cause exception while read , service is {}", getService(), e);
            } else {
                LOGGER.debug("cause exception while read {}, service is {}", e.toString(), getService());
            }

            graceClosedReasons.add(e.toString());
            this.socketWR.disableRead();
            pushInnerServiceTask(ServiceTaskFactory.getInstance(getService()).createForGracefulClose(graceClosedReasons));
        }
    }

    public void doNextWriteCheck() {
        this.socketWR.doNextWriteCheck();
    }

    public abstract void setProcessor(IOProcessor processor);

    public void setId(long id) {
        this.id = id;
    }

    public boolean isFlowControlled() {
        return flowControlled;
    }

    public void setFlowControlled(boolean flowControlled) {
        this.flowControlled = flowControlled;
    }

    public abstract void startFlowControl();

    public abstract void stopFlowControl();

    public int getLocalPort() {
        return localPort;
    }

    public void setLocalPort(int localPort) {
        this.localPort = localPort;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public synchronized void setService(AbstractService service) {
        this.service = service;
    }

    public int getReadBufferChunk() {
        return readBufferChunk;
    }

    public void setReadBufferChunk(int readBufferChunk) {
        this.readBufferChunk = readBufferChunk;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    public String getCloseReason() {
        return closeReason;
    }

    public IOProcessor getProcessor() {
        return processor;
    }

    public long getNetInBytes() {
        return netInBytes;
    }

    public long getNetOutBytes() {
        return netOutBytes;
    }

    public long getStartupTime() {
        return startupTime;
    }

    public long getLastReadTime() {
        return lastReadTime;
    }

    public long getLastWriteTime() {
        return lastWriteTime;
    }

    public void setSupportCompress(boolean supportCompress) {
        this.isSupportCompress = supportCompress;
    }

    public ProtoHandler getProto() {
        return proto;
    }

    public void setProto(ProtoHandler proto) {
        this.proto = proto;
    }

}
