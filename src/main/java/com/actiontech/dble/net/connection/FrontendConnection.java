package com.actiontech.dble.net.connection;

import com.actiontech.dble.backend.mysql.proto.handler.Impl.SSLProtoHandler;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResultCode;
import com.actiontech.dble.btrace.provider.IODelayProvider;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.factory.SSLEngineFactory;
import com.actiontech.dble.net.service.*;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.FrontendService;
import com.actiontech.dble.services.mysqlauthenticate.MySQLChangeUserService;
import com.actiontech.dble.singleton.FlowController;
import com.actiontech.dble.util.TimeUtil;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by szf on 2020/6/23.
 */
public class FrontendConnection extends AbstractConnection {

    private static final long AUTH_TIMEOUT = 15 * 1000L;

    private final boolean isManager;
    private final long idleTimeout;
    private final AtomicBoolean isCleanUp;
    //skip idleTimeout checks
    private boolean skipCheck;

    private final boolean isSupportSSL;
    protected volatile ByteBuffer netReadBuffer;
    private volatile SSLHandler sslHandler;

    public FrontendConnection(NetworkChannel channel, SocketWR socketWR, boolean isManager) throws IOException {
        super(channel, socketWR);
        this.isManager = isManager;
        InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
        InetSocketAddress remoteAddress;
        if (channel instanceof SocketChannel) {
            remoteAddress = (InetSocketAddress) ((SocketChannel) channel).getRemoteAddress();
        } else if (channel instanceof AsynchronousSocketChannel) {
            remoteAddress = (InetSocketAddress) ((AsynchronousSocketChannel) channel).getRemoteAddress();
        } else {
            throw new RuntimeException("FrontendConnection type is" + channel.getClass());
        }
        this.host = remoteAddress.getHostString();
        this.port = localAddress.getPort();
        this.localPort = remoteAddress.getPort();
        this.idleTimeout = SystemConfig.getInstance().getIdleTimeout();
        this.isCleanUp = new AtomicBoolean(false);
        this.isSupportSSL = SSLEngineFactory.getInstance().isSupport();
    }

    public void openSSL() throws IOException {
        sslHandler = new SSLHandler(this);
        sslHandler.init();
    }

    public void doSSLHandShake(byte[] data) {
        try {
            if (!isUseSSL()) {
                close("SSL not initialized");
                return;
            }
            sslHandler.handShake(data);
        } catch (SSLException e) {
            LOGGER.error("SSL handshake failed, exception: {},", e);
            close("SSL handshake failed");
        }
        return;
    }

    @Override
    public void handle(ByteBuffer dataBuffer) throws IOException {
        if (isSupportSSL && isUseSSL()) {
            handleSSLData(dataBuffer);
        } else {
            transferToReadBuffer(dataBuffer);
            parentHandle(readBuffer);
        }
    }

    private void transferToReadBuffer(ByteBuffer dataBuffer) {
        if (!isSupportSSL) return;
        dataBuffer.flip();
        ByteBuffer readBuffer = findReadBuffer();
        int len = readBuffer.position() + dataBuffer.limit();
        if (readBuffer.capacity() < len) {
            readBuffer = ensureReadBufferFree(readBuffer, len);
        }
        readBuffer.put(dataBuffer);
        dataBuffer.clear();
    }

    public void parentHandle(ByteBuffer buffer) throws IOException {
        super.handle(buffer);
    }

    public void handleSSLData(ByteBuffer dataBuffer) throws IOException {
        if (dataBuffer == null) {
            return;
        }
        int offset = 0;
        SSLProtoHandler proto = new SSLProtoHandler();
        boolean hasRemaining = true;
        while (hasRemaining) {
            ProtoHandlerResult result = proto.handle(dataBuffer, offset, false);
            switch (result.getCode()) {
                case SSL_PROTO_PACKET:
                case SSL_CLOSE_PACKET:
                    if (!result.isHasMorePacket()) {
                        netReadReachEnd();
                        if (readBuffer != null) {
                            readBuffer.clear();
                        }
                    }
                    processSSLProto(result.getPacketData(), result.getCode());
                    if (!result.isHasMorePacket()) {
                        dataBuffer.clear();
                    }
                    break;
                case SSL_APP_PACKET:
                    if (!result.isHasMorePacket()) {
                        netReadReachEnd();
                    }
                    processSSLAppData(result.getPacketData());
                    if (!result.isHasMorePacket()) {
                        dataBuffer.clear();
                    }
                    break;
                case BUFFER_PACKET_UNCOMPLETE:
                    processSSLPacketUnComplete(dataBuffer, offset);
                    break;
                case SSL_BUFFER_NOT_BIG_ENOUGH:
                    processSSLPacketNotBigEnough(dataBuffer, result.getOffset(), result.getPacketLength());
                    break;
                default:
                    break;
            }
            hasRemaining = result.isHasMorePacket();
            if (hasRemaining) {
                offset = result.getOffset();
            }
        }
    }

    private void netReadReachEnd() {
        // if cur buffer is temper none direct byte buffer and not
        // received large message in recent 30 seconds
        // then change to direct buffer for performance
        ByteBuffer localReadBuffer = netReadBuffer;
        if (localReadBuffer != null && !localReadBuffer.isDirect() &&
                lastLargeMessageTime < lastReadTime - 30 * 1000L) {  // used temp heap
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("change to direct con read buffer ,cur temp buf size :" + localReadBuffer.capacity());
            }
            recycle(localReadBuffer);
            netReadBuffer = processor.getBufferPool().allocate(readBufferChunk);
        } else {
            if (localReadBuffer != null) {
                IODelayProvider.inReadReachEnd();
                localReadBuffer.clear();
            }
        }
    }

    private void processSSLProto(byte[] packetData, ProtoHandlerResultCode code) {
        AbstractService frontService = getService();
        if (packetData != null) {
            if (code == ProtoHandlerResultCode.SSL_PROTO_PACKET) {
                pushServiceTask(new SSLProtoServerTask(packetData, frontService));
            } else {
                pushServiceTask(ServiceTaskFactory.getInstance(frontService).createForGracefulClose("ssl close", CloseType.READ));
            }
        }
    }

    private void processSSLAppData(byte[] packetData) throws IOException {
        if (packetData == null)
            return;
        sslHandler.unwrapAppData(packetData);
        parentHandle(readBuffer);
    }

    public void processSSLPacketNotBigEnough(ByteBuffer buffer, int offset, final int pkgLength) {
        ByteBuffer newBuffer = allocate(pkgLength);
        buffer.position(offset);
        newBuffer.put(buffer);
        this.netReadBuffer = newBuffer;
        recycle(buffer);
    }

    private void processSSLPacketUnComplete(ByteBuffer buffer, int offset) {
        if (buffer == null) {
            return;
        }
        buffer.limit(buffer.position());
        buffer.position(offset);
        netReadBuffer = buffer.compact();
    }

    @Override
    public void businessClose(String reason) {
        this.close(reason);
    }

    @Override
    public void close(String reason) {
        if (isUseSSL())
            sslHandler.close();
        super.close(reason);
    }

    @Override
    public synchronized void recycleReadBuffer() {
        if (netReadBuffer != null) {
            this.recycle(netReadBuffer);
            this.netReadBuffer = null;
        }
        super.recycleReadBuffer();
    }

    @Override
    public void startFlowControl(int currentWritingSize) {
        if (!frontWriteFlowControlled && this.getService() instanceof BusinessService &&
                currentWritingSize > FlowController.getFlowHighLevel()) {
            ((BusinessService) this.getService()).getSession().startFlowControl(currentWritingSize);
        }
    }

    @Override
    public void stopFlowControl(int currentWritingSize) {
        if (this.getService() instanceof BusinessService &&
                currentWritingSize <= FlowController.getFlowLowLevel()) {
            ((BusinessService) this.getService()).getSession().stopFlowControl(currentWritingSize);
        }
    }

    @Override
    public void cleanup(String reason) {
        if (isCleanUp.compareAndSet(false, true)) {
            if (netReadBuffer != null) {
                this.recycle(netReadBuffer);
                this.netReadBuffer = null;
            }
            super.cleanup(reason);
            AbstractService service = getService();
            if (service instanceof FrontendService) {
                ((FrontendService) service).userConnectionCount();
            }
        }
    }

    @Override
    public void setProcessor(IOProcessor processor) {
        this.processor = processor;
        processor.addFrontend(this);
    }

    @Override
    public ByteBuffer wrap(ByteBuffer orgBuffer) throws SSLException {
        if (!isUseSSL())
            return orgBuffer;
        return sslHandler.wrapAppData(orgBuffer);
    }

    @Override
    public void compactReadBuffer(ByteBuffer dataBuffer, int offset) throws IOException {
        if (dataBuffer == null) {
            return;
        }
        if (isSupportSSL && SSLProtoHandler.isSSLPackage(dataBuffer, offset)) {
            dataBuffer.flip();
            dataBuffer.position(offset);
            int len = netReadBuffer.position() + (dataBuffer.limit() - dataBuffer.position());
            if (netReadBuffer.capacity() < len) {
                processSSLPacketNotBigEnough(netReadBuffer, 0, len);
            }
            this.netReadBuffer.put(dataBuffer);
            dataBuffer.clear();
            handleSSLData(netReadBuffer);
        } else {
            dataBuffer.limit(dataBuffer.position());
            dataBuffer.position(offset);
            this.readBuffer = dataBuffer.compact();
        }
    }

    public ByteBuffer findReadBuffer() {
        if (readBuffer == null) {
            readBuffer = processor.getBufferPool().allocate(processor.getBufferPool().getChunkSize());
        }
        return readBuffer;
    }

    public ByteBuffer ensureReadBufferFree(ByteBuffer oldBuffer, int expectSize) {
        ByteBuffer newBuffer = processor.getBufferPool().allocate(expectSize < 0 ? processor.getBufferPool().getChunkSize() : expectSize);
        oldBuffer.flip();
        newBuffer.put(oldBuffer);
        readBuffer = newBuffer;

        oldBuffer.clear();
        recycle(oldBuffer);

        return newBuffer;
    }

    public boolean isIdleTimeout() {
        if (!(getService() instanceof AuthService)) {
            if (isManager && skipCheck) {
                //split
                return false;
            } else if (isSkipCheck() && (lastReadTime > lastWriteTime)) {
                return false;
            }
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + AUTH_TIMEOUT;
        }
    }

    public ByteBuffer findNetReadBuffer() {
        if (isSupportSSL) {
            if (this.netReadBuffer == null) {
                netReadBuffer = processor.getBufferPool().allocate(processor.getBufferPool().getChunkSize());
            }
            return netReadBuffer;
        } else {
            return super.findNetReadBuffer();
        }
    }

    public boolean isManager() {
        return isManager;
    }

    public FrontendService getFrontEndService() {
        return (FrontendService) getService();
    }

    public boolean isAuthorized() {
        return !(getService() instanceof AuthService) && !(getService() instanceof MySQLChangeUserService);
    }

    public boolean isSkipCheck() {
        return skipCheck;
    }

    public void setSkipCheck(boolean skipCheck) {
        updateLastReadTime();
        this.skipCheck = skipCheck;
    }

    public boolean isUseSSL() {
        return sslHandler != null;
    }

    public String toString() {
        return "FrontendConnection[id = " + id + " port = " + port + " host = " + host + " local_port = " + localPort + " isManager = " + isManager() + " startupTime = " + startupTime + " skipCheck = " + isSkipCheck() + " isFlowControl = " + isFrontWriteFlowControlled() + " onlyTcpConnect = " + isOnlyFrontTcpConnected() + " useSSL = " + isUseSSL() + "]";
    }
}
