/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.SystemVariables;
import com.actiontech.dble.util.CompressUtil;
import com.actiontech.dble.util.TimeUtil;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.NetworkChannel;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author mycat
 */
public abstract class AbstractConnection implements NIOConnection {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnection.class);

    protected String host;
    protected int localPort;
    protected int port;
    protected long id;
    protected volatile CharsetNames charsetName = new CharsetNames();

    protected final NetworkChannel channel;
    protected NIOProcessor processor;
    protected NIOHandler handler;
    protected int readBufferChunk;
    protected int maxPacketSize;
    protected volatile ByteBuffer readBuffer;
    protected volatile ByteBuffer writeBuffer;

    protected final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();

    protected volatile int readBufferOffset;
    protected long lastLargeMessageTime;
    protected final AtomicBoolean isClosed;
    protected long startupTime;
    protected long lastReadTime;
    protected long lastWriteTime;
    protected long netInBytes;
    protected long netOutBytes;

    protected volatile boolean isSupportCompress = false;
    protected final ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue = new ConcurrentLinkedQueue<>();
    protected final ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue = new ConcurrentLinkedQueue<>();

    private long idleTimeout;

    private final SocketWR socketWR;

    public AbstractConnection(NetworkChannel channel) {
        this.channel = channel;
        boolean isAIO = (channel instanceof AsynchronousChannel);
        if (isAIO) {
            socketWR = new AIOSocketWR(this);
        } else {
            socketWR = new NIOSocketWR(this);
        }
        this.isClosed = new AtomicBoolean(false);
        this.startupTime = TimeUtil.currentTimeMillis();
        this.lastReadTime = startupTime;
        this.lastWriteTime = startupTime;
    }

    public AbstractConnection() {
        /* just for unit test */
        this.channel = null;
        this.isClosed = new AtomicBoolean(false);
        this.socketWR = null;
    }
    public boolean setCollationConnection(String collation) {
        int ci = CharsetUtil.getCollationIndex(collation);
        if (ci <= 0) {
            return false;
        }
        charsetName.setCollation(collation);
        return true;
    }

    public boolean setCharacterConnection(String charset) {
        String collationName = CharsetUtil.getDefaultCollation(charset);
        if (collationName == null) {
            return false;
        }
        charsetName.setCollation(collationName);
        return true;
    }

    public boolean setCharacterResults(String name) {
        int ci = CharsetUtil.getCharsetDefaultIndex(name);
        if (ci <= 0 && !name.equals("null")) {
            return false;
        }
        charsetName.setResults(name);
        return true;
    }


    public void setCharsetName(CharsetNames charsetName) {
        this.charsetName = charsetName.clone();
    }

    public boolean setCharacterClient(String name) {
        int ci = CharsetUtil.getCharsetDefaultIndex(name);
        if (ci <= 0) {
            return false;
        }
        charsetName.setClient(name);
        return true;
    }

    public boolean setCharacterSet(String name) {
        int ci = CharsetUtil.getCharsetDefaultIndex(name);
        if (ci <= 0) {
            return false;
        }
        charsetName.setClient(name);
        charsetName.setResults(name);
        charsetName.setCollation(SystemVariables.getDefaultValue("collation_database"));
        return true;
    }

    public boolean setNames(String name, String collationName) {
        int ci = CharsetUtil.getCharsetDefaultIndex(name);
        if (ci <= 0) {
            return false;
        }
        if (collationName == null) {
            collationName = CharsetUtil.getDefaultCollation(name);
        } else if (CharsetUtil.getCollationIndex(collationName) <= 0) {
            return false;
        }
        charsetName.setNames(name, collationName);
        return true;
    }

    public CharsetNames getCharset() {
        return charsetName;
    }

    public boolean isSupportCompress() {
        return isSupportCompress;
    }

    public void setSupportCompress(boolean supportCompress) {
        this.isSupportCompress = supportCompress;
    }

    public SocketWR getSocketWR() {
        return socketWR;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public int getLocalPort() {
        return localPort;
    }

    public String getHost() {
        return host;
    }


    public int getPort() {
        return port;
    }

    public void setLocalPort(int localPort) {
        this.localPort = localPort;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public boolean isIdleTimeout() {
        return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
    }

    public NetworkChannel getChannel() {
        return channel;
    }


    public void setReadBufferChunk(int readBufferChunk) {
        this.readBufferChunk = readBufferChunk;
    }

    public void setMaxPacketSize(int maxPacketSize) {
        this.maxPacketSize = maxPacketSize;
    }

    public long getStartupTime() {
        return startupTime;
    }

    public long getLastReadTime() {
        return lastReadTime;
    }

    public void setProcessor(NIOProcessor processor) {
        this.processor = processor;
        int size = processor.getBufferPool().getChunkSize();
        this.readBuffer = processor.getBufferPool().allocate(size);
    }

    public long getLastWriteTime() {
        return lastWriteTime;
    }

    public long getNetInBytes() {
        return netInBytes;
    }

    public long getNetOutBytes() {
        return netOutBytes;
    }

    public NIOProcessor getProcessor() {
        return processor;
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }

    public ByteBuffer allocate() {
        int size = this.processor.getBufferPool().getChunkSize();
        ByteBuffer buffer = this.processor.getBufferPool().allocate(size);
        return buffer;
    }

    public final void recycle(ByteBuffer buffer) {
        this.processor.getBufferPool().recycle(buffer);
    }

    public void setHandler(NIOHandler handler) {
        this.handler = handler;
    }

    @Override
    public void handle(byte[] data) {
        if (isSupportCompress()) {
            List<byte[]> packs = CompressUtil.decompressMysqlPacket(data, decompressUnfinishedDataQueue);
            for (byte[] pack : packs) {
                if (pack.length != 0) {
                    handler.handle(pack);
                }
            }
        } else {
            handler.handle(data);
        }
    }

    @Override
    public void register() throws IOException {

    }

    public void asynRead() throws IOException {
        this.socketWR.asynRead();
    }

    public void doNextWriteCheck() throws IOException {
        this.socketWR.doNextWriteCheck();
    }

    public void onReadData(int got) throws IOException {
        if (isClosed.get()) {
            return;
        }

        lastReadTime = TimeUtil.currentTimeMillis();
        if (got < 0) {
            this.close("stream closed");
            return;
        } else if (got == 0 && !this.channel.isOpen()) {
            this.close("socket closed");
            return;
        }
        netInBytes += got;
        processor.addNetInBytes(got);

        // execute data in loop
        int offset = readBufferOffset, length = 0, position = readBuffer.position();
        for (; ; ) {
            length = getPacketLength(readBuffer, offset);
            if (length == -1) {
                if (offset != 0) {
                    this.readBuffer = compactReadBuffer(readBuffer, offset);
                } else if (readBuffer != null && !readBuffer.hasRemaining()) {
                    throw new RuntimeException("invalid readbuffer capacity ,too little buffer size " +
                            readBuffer.capacity());
                }
                break;
            }
            if (position >= offset + length && readBuffer != null) {
                // handle this package
                readBuffer.position(offset);
                byte[] data = new byte[length];
                readBuffer.get(data, 0, length);
                handle(data);
                // maybe handle stmt_close
                if (isClosed()) {
                    return;
                }
                // offset to next position
                offset += length;
                // reached end
                if (position == offset) {
                    readReachEnd();
                    break;
                } else {
                    // try next package parse
                    readBufferOffset = offset;
                    if (readBuffer != null) {
                        readBuffer.position(position);
                    }
                    continue;
                }
            } else {
                // not read whole message package ,so check if buffer enough and
                // compact readbuffer
                if (!readBuffer.hasRemaining()) {
                    readBuffer = ensureFreeSpaceOfReadBuffer(readBuffer, offset, length);
                }
                break;
            }
        }
    }

    private void readReachEnd() {
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
        // no more data ,break
        readBufferOffset = 0;
    }


    private ByteBuffer ensureFreeSpaceOfReadBuffer(ByteBuffer buffer,
                                                   int offset, final int pkgLength) {
        // need a large buffer to hold the package
        if (pkgLength > maxPacketSize) {
            throw new IllegalArgumentException("Packet size over the limit.");
        } else if (buffer.capacity() < pkgLength) {

            ByteBuffer newBuffer = processor.getBufferPool().allocate(pkgLength);
            lastLargeMessageTime = TimeUtil.currentTimeMillis();
            buffer.position(offset);
            newBuffer.put(buffer);
            readBuffer = newBuffer;

            recycle(buffer);
            readBufferOffset = 0;
            return newBuffer;

        } else {
            if (offset != 0) {
                // compact bytebuffer only
                return compactReadBuffer(buffer, offset);
            } else {
                throw new RuntimeException(" not enough space");
            }
        }
    }

    private ByteBuffer compactReadBuffer(ByteBuffer buffer, int offset) {
        if (buffer == null) {
            return null;
        }
        buffer.limit(buffer.position());
        buffer.position(offset);
        buffer = buffer.compact();
        readBufferOffset = 0;
        return buffer;
    }

    public void write(byte[] data) {
        ByteBuffer buffer = allocate();
        buffer = writeToBuffer(data, buffer);
        write(buffer);

    }

    @Override
    public final void write(ByteBuffer buffer) {

        if (isSupportCompress()) {
            ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer, this, compressUnfinishedDataQueue);
            writeQueue.offer(newBuffer);
        } else {
            writeQueue.offer(buffer);
        }

        // if ansyn write finishe event got lock before me ,then writing
        // flag is set false but not start a write request
        // so we check again
        try {
            this.socketWR.doNextWriteCheck();
        } catch (Exception e) {
            LOGGER.warn("write err:", e);
            this.close("write err:" + e);
        }
    }

    private void writeNotSend(ByteBuffer buffer) {
        if (isSupportCompress()) {
            ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer, this, compressUnfinishedDataQueue);
            writeQueue.offer(newBuffer);

        } else {
            writeQueue.offer(buffer);
        }
    }


    public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity, boolean writeSocketIfFull) {
        if (capacity > buffer.remaining()) {
            if (writeSocketIfFull) {
                writeNotSend(buffer);
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
                writeNotSend(buffer);
                buffer = allocate();
                offset += remaining;
                length -= remaining;
                remaining = buffer.remaining();
                continue;
            }
        }
        return buffer;
    }

    @Override
    public void close(String reason) {
        if (!isClosed.get()) {
            closeSocket();
            isClosed.set(true);
            if (processor != null) {
                processor.removeConnection(this);
            }
            this.cleanup();
            isSupportCompress = false;

            // ignore null information
            if (Strings.isNullOrEmpty(reason)) {
                return;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("close connection,reason:" + reason + " ," + this);
            }
            if (reason.contains("connection,reason:java.net.ConnectException")) {
                throw new RuntimeException(" errr");
            }
        }
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    public void idleCheck() {
        if (isIdleTimeout()) {
            LOGGER.info(toString() + " idle timeout");
            close(" idle ");
        }
    }

    protected void cleanup() {

        if (readBuffer != null) {
            this.recycle(readBuffer);
            this.readBuffer = null;
            this.readBufferOffset = 0;
        }

        if (writeBuffer != null) {
            recycle(writeBuffer);
            this.writeBuffer = null;
        }

        if (!decompressUnfinishedDataQueue.isEmpty()) {
            decompressUnfinishedDataQueue.clear();
        }

        if (!compressUnfinishedDataQueue.isEmpty()) {
            compressUnfinishedDataQueue.clear();
        }

        ByteBuffer buffer = null;
        while ((buffer = writeQueue.poll()) != null) {
            recycle(buffer);
        }
    }

    protected int getPacketLength(ByteBuffer buffer, int offset) {
        int headerSize = MySQLPacket.PACKET_HEADER_SIZE;
        if (isSupportCompress()) {
            headerSize = 7;
        }

        if (buffer.position() < offset + headerSize) {
            return -1;
        } else {
            int length = buffer.get(offset) & 0xff;
            length |= (buffer.get(++offset) & 0xff) << 8;
            length |= (buffer.get(++offset) & 0xff) << 16;
            return length + headerSize;
        }
    }

    public ConcurrentLinkedQueue<ByteBuffer> getWriteQueue() {
        return writeQueue;
    }

    private void closeSocket() {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
                LOGGER.error("AbstractConnectionCloseError", e);
            }

            boolean closed = !channel.isOpen();
            if (!closed) {
                LOGGER.warn("close socket of connnection failed " + this);
            }
        }
    }

    public void onConnectfinish() {
        LOGGER.debug("The backend conntinon has finished connecting");
    }
}
