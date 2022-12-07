/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.CompressUtil;
import com.actiontech.dble.util.TimeUtil;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.NetworkChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    protected volatile boolean isClosed = false;
    protected long startupTime;
    protected long lastReadTime;
    protected long lastWriteTime;
    protected long netInBytes;
    protected long netOutBytes;

    protected volatile boolean isSupportCompress = false;
    protected final ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue = new ConcurrentLinkedQueue<>();
    protected final ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue = new ConcurrentLinkedQueue<>();
    protected volatile Map<String, String> usrVariables;
    protected volatile Map<String, String> sysVariables;

    private long idleTimeout;

    private final SocketWR socketWR;

    private byte[] rowData;

    private volatile boolean flowControlled;

    public AbstractConnection(NetworkChannel channel) {
        this.channel = channel;
        boolean isAIO = (channel instanceof AsynchronousChannel);
        if (isAIO) {
            socketWR = new AIOSocketWR(this);
        } else {
            socketWR = new NIOSocketWR(this);
        }
        this.startupTime = TimeUtil.currentTimeMillis();
        this.lastReadTime = startupTime;
        this.lastWriteTime = startupTime;
    }

    public AbstractConnection() {
        /* just for unit test */
        this.channel = null;
        this.socketWR = null;
    }

    public void setCollationConnection(String collation) {
        charsetName.setCollation(collation);
    }

    public void setCharacterConnection(String collationName) {
        charsetName.setCollation(collationName);
    }

    public void setCharacterResults(String name) {
        charsetName.setResults(name);
    }


    public void setCharsetName(CharsetNames charsetName) {
        this.charsetName = charsetName.copyObj();
    }

    public void setCharacterClient(String name) {
        charsetName.setClient(name);
    }

    public void setCharacterSet(String name) {
        charsetName.setClient(name);
        charsetName.setResults(name);
        charsetName.setCollation(DbleServer.getInstance().getSystemVariables().getDefaultValue("collation_database"));
    }


    public void initCharacterSet(String name) {
        charsetName.setClient(name);
        charsetName.setResults(name);
        charsetName.setCollation(CharsetUtil.getDefaultCollation(name));
    }

    public void setNames(String name, String collationName) {
        charsetName.setNames(name, collationName);
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

    public Map<String, String> getUsrVariables() {
        return usrVariables;
    }

    public Map<String, String> getSysVariables() {
        return sysVariables;
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
        return this.processor.getBufferPool().allocate(size);
    }

    public ByteBuffer allocate(int size) {
        return this.processor.getBufferPool().allocate(size);
    }

    public final void recycle(ByteBuffer buffer) {
        this.processor.getBufferPool().recycle(buffer);
    }

    public NIOHandler getHandler() {
        return handler;
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

    public void asyncRead() throws IOException {
        this.socketWR.asyncRead();
    }

    public void doNextWriteCheck() throws IOException {
        this.socketWR.doNextWriteCheck();
    }

    public void onReadData(int got) throws IOException {
        if (isClosed) {
            return;
        }

        lastReadTime = TimeUtil.currentTimeMillis();
        if (lastReadTime == lastWriteTime) {
            lastWriteTime--;
        }
        if (got < 0) {
            if (this instanceof MySQLConnection) {
                ((MySQLConnection) this).closeInner("stream closed");
            } else {
                this.close("stream closed");
            }
            return;
        } else if (got == 0 && !this.channel.isOpen()) {
            if (this instanceof MySQLConnection) {
                ((MySQLConnection) this).closeInner("stream closed");
            } else {
                this.close("stream closed");
            }
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
                data = checkData(data, length);
                if (data == null) {
                    return;
                }
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
        if (buffer.capacity() < pkgLength) {
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
    public void write(ByteBuffer buffer) {
        if (isClosed) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("it will not write because of closed " + this);
            }
            if (buffer != null) {
                recycle(buffer);
            }
            this.cleanup();
            return;
        }
        if (isSupportCompress()) {
            ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer, this, compressUnfinishedDataQueue);
            writeQueue.offer(newBuffer);
        } else {
            writeQueue.offer(buffer);
        }

        // if ansyn write finished event got lock before me ,then writing
        // flag is set false but not start a write request
        // so we check again
        try {
            this.socketWR.doNextWriteCheck();
        } catch (Exception e) {
            LOGGER.info("write err:", e);
            this.close("write err:" + e);
        }
    }

    public final boolean registerWrite(ByteBuffer buffer) {

        // if ansyn write finished event got lock before me ,then writing
        // flag is set false but not start a write request
        // so we check again
        try {
            return this.socketWR.registerWrite(buffer);
        } catch (Exception e) {
            LOGGER.info("write err:", e);
            this.close("write err:" + e);
            return false;
        }
    }

    public void writePart(ByteBuffer buffer) {
        write(buffer);
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

    public ByteBuffer writeBigPackageToBuffer(byte[] row, ByteBuffer buffer, byte packetId) {
        int srcPos;
        byte[] singlePacket;
        singlePacket = new byte[MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE];
        System.arraycopy(row, 0, singlePacket, 0, MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE);
        srcPos = MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE;
        int length = row.length;
        length -= (MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE);
        ByteUtil.writeUB3(singlePacket, MySQLPacket.MAX_PACKET_SIZE);
        singlePacket[3] = ++packetId;
        buffer = writeToBuffer(singlePacket, buffer);
        while (length >= MySQLPacket.MAX_PACKET_SIZE) {
            singlePacket = new byte[MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE];
            ByteUtil.writeUB3(singlePacket, MySQLPacket.MAX_PACKET_SIZE);
            singlePacket[3] = ++packetId;
            System.arraycopy(row, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, MySQLPacket.MAX_PACKET_SIZE);
            srcPos += MySQLPacket.MAX_PACKET_SIZE;
            length -= MySQLPacket.MAX_PACKET_SIZE;
            buffer = writeToBuffer(singlePacket, buffer);
        }
        singlePacket = new byte[length + MySQLPacket.PACKET_HEADER_SIZE];
        ByteUtil.writeUB3(singlePacket, length);
        singlePacket[3] = ++packetId;
        System.arraycopy(row, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, length);
        buffer = writeToBuffer(singlePacket, buffer);
        ((ServerConnection) this).getSession2().getPacketId().set(packetId);
        return buffer;
    }


    public abstract void connectionCount();

    @Override
    public void close(String reason) {
        if (!isClosed) {
            this.connectionCount();
            closeSocket();
            isClosed = true;
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
                throw new RuntimeException(reason);
            }
        } else {
            // make sure buffer recycle again, avoid buffer leak
            this.cleanup();
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void idleCheck() {
        if (isIdleTimeout()) {
            LOGGER.info(toString() + " idle timeout");
            close(" idle ");
        }
    }

    protected synchronized void cleanup() {

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
        ByteBuffer buffer;
        while ((buffer = writeQueue.poll()) != null) {
            recycle(buffer);
        }
    }

    private int getPacketLength(ByteBuffer buffer, int offset) {
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
                LOGGER.info("AbstractConnectionCloseError", e);
            }

            boolean closed = !channel.isOpen();
            if (!closed) {
                LOGGER.info("close socket of connnection failed " + this);
            }
        }
    }

    public String getStringOfSysVariables() {
        StringBuilder sbSysVariables = new StringBuilder();
        int cnt = 0;
        if (sysVariables != null) {
            for (Map.Entry sysVariable : sysVariables.entrySet()) {
                if (cnt > 0) {
                    sbSysVariables.append(",");
                }
                sbSysVariables.append(sysVariable.getKey());
                sbSysVariables.append("=");
                sbSysVariables.append(sysVariable.getValue());
                cnt++;
            }
        }
        return sbSysVariables.toString();
    }

    public String getStringOfUsrVariables() {
        StringBuilder sbUsrVariables = new StringBuilder();
        int cnt = 0;
        if (usrVariables != null) {
            for (Map.Entry usrVariable : usrVariables.entrySet()) {
                if (cnt > 0) {
                    sbUsrVariables.append(",");
                }
                sbUsrVariables.append(usrVariable.getKey());
                sbUsrVariables.append("=");
                sbUsrVariables.append(usrVariable.getValue());
                cnt++;
            }
        }
        return sbUsrVariables.toString();
    }

    public void onConnectFinish() {
        LOGGER.debug("The backend conntinon has finished connecting");
    }

    public void setSocketParams(boolean isFrontChannel) throws IOException {
        SystemConfig system = DbleServer.getInstance().getConfig().getSystem();
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

        this.setMaxPacketSize(system.getMaxPacketSize());
        this.setIdleTimeout(system.getIdleTimeout());
        this.initCharacterSet(system.getCharset());
        this.setReadBufferChunk(soRcvBuf);
    }

    private byte[] checkData(byte[] data, int length) {
        if (this instanceof ServerConnection) {
            NonBlockingSession session = ((ServerConnection) this).getSession2();
            if (session != null) {
                session.getPacketId().set(data[3]);
            }
        }
        if (length >= MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE) {
            if (rowData == null) {
                rowData = data;
            } else {
                byte[] nextData = new byte[data.length - MySQLPacket.PACKET_HEADER_SIZE];
                System.arraycopy(data, MySQLPacket.PACKET_HEADER_SIZE, nextData, 0, data.length - MySQLPacket.PACKET_HEADER_SIZE);
                rowData = dataMerge(nextData);
            }
            readReachEnd();
            return null;
        } else {
            if (rowData != null) {
                byte[] nextData = new byte[data.length - MySQLPacket.PACKET_HEADER_SIZE];
                System.arraycopy(data, MySQLPacket.PACKET_HEADER_SIZE, nextData, 0, data.length - MySQLPacket.PACKET_HEADER_SIZE);
                rowData = dataMerge(nextData);
                data = rowData;
                rowData = null;
            }
            return data;
        }
    }

    private byte[] dataMerge(byte[] data) {
        byte[] newData = new byte[rowData.length + data.length];
        System.arraycopy(rowData, 0, newData, 0, rowData.length);
        System.arraycopy(data, 0, newData, rowData.length, data.length);
        return newData;
    }

    public boolean isFlowControlled() {
        return flowControlled;
    }

    public void setFlowControlled(boolean flowControlled) {
        this.flowControlled = flowControlled;
    }

    /*
    start flow control because of the write queue in this connection to long

     */
    public abstract void startFlowControl(BackendConnection bcon);

    public abstract void stopFlowControl();
}
