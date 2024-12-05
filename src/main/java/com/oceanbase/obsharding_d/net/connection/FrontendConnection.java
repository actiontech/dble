/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.connection;

import com.oceanbase.obsharding_d.backend.mysql.proto.handler.Impl.SSLProtoHandler;
import com.oceanbase.obsharding_d.backend.mysql.proto.handler.ProtoHandlerResult;
import com.oceanbase.obsharding_d.btrace.provider.IODelayProvider;
import com.oceanbase.obsharding_d.buffer.BufferType;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.SocketWR;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.AuthService;
import com.oceanbase.obsharding_d.net.ssl.OpenSSLWrapper;
import com.oceanbase.obsharding_d.net.ssl.SSLWrapperRegistry;
import com.oceanbase.obsharding_d.services.BusinessService;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.mysqlauthenticate.MySQLChangeUserService;
import com.oceanbase.obsharding_d.singleton.FlowController;
import com.oceanbase.obsharding_d.util.TimeUtil;
import com.oceanbase.obsharding_d.util.exception.NotSupportException;

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
    private String sslName;

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
        this.isSupportSSL = SystemConfig.getInstance().isSupportSSL();
    }

    @Override
    protected void handle(ByteBuffer dataBuffer, boolean isContainSSLData) throws IOException {
        if (this.isSupportSSL && isUseSSL()) {
            //after ssl-client hello
            handleSSLData(dataBuffer);
        } else {
            //ssl buffer -> bottomRead buffer
            transferToReadBuffer(dataBuffer);
            if (maybeUseSSL()) {
                //ssl login request(non ssl)&client hello(ssl)
                super.handle(getBottomReadBuffer(), true);
            } else {
                //no ssl
                handleNonSSL(getBottomReadBuffer());
            }
        }
    }

    protected void handleNonSSL(ByteBuffer dataBuffer) throws IOException {
        super.handle(dataBuffer, false);
    }

    @Override
    public void initSSLContext(int protocol) {
        if (sslHandler != null) {
            return;
        }
        sslHandler = new SSLHandler(this);
        OpenSSLWrapper sslWrapper = SSLWrapperRegistry.getInstance(protocol);
        if (sslWrapper == null) {
            throw new NotSupportException("not support " + SSLWrapperRegistry.SSLProtocol.nameOf(protocol));
        }
        sslName = SSLWrapperRegistry.SSLProtocol.nameOf(protocol);
        sslHandler.setSslWrapper(sslWrapper);
    }

    public void doSSLHandShake(byte[] data) {
        try {
            if (!isUseSSL()) {
                close("SSL not initialized");
                return;
            }
            if (!sslHandler.isCreateEngine()) {
                sslHandler.createEngine();
            }
            sslHandler.handShake(data);
        } catch (SSLException e) {
            LOGGER.warn("SSL handshake failed, exception: ", e);
            close("SSL handshake failed");
        } catch (IOException e) {
            LOGGER.warn("SSL initialization failed, exception: ", e);
            close("SSL initialization failed");
        }
    }

    private void transferToReadBuffer(ByteBuffer dataBuffer) {
        if (!isSupportSSL || !maybeUseSSL()) return;
        dataBuffer.flip();
        ByteBuffer readBuffer = findBottomReadBuffer();
        int len = readBuffer.position() + dataBuffer.limit();
        if (readBuffer.capacity() < len) {
            readBuffer = ensureReadBufferFree(readBuffer, len);
        }
        readBuffer.put(dataBuffer);
        dataBuffer.clear();
    }

    public void handleSSLData(ByteBuffer dataBuffer) throws IOException {
        if (dataBuffer == null) {
            return;
        }
        int offset = 0;
        SSLProtoHandler proto = new SSLProtoHandler(this);
        boolean hasRemaining = true;
        while (hasRemaining) {
            ProtoHandlerResult result = proto.handle(dataBuffer, offset, false, true);
            switch (result.getCode()) {
                case SSL_PROTO_PACKET:
                case SSL_CLOSE_PACKET:
                    if (!result.isHasMorePacket()) {
                        netReadReachEnd();
                        final ByteBuffer tmpReadBuffer = getBottomReadBuffer();
                        if (tmpReadBuffer != null) {
                            tmpReadBuffer.clear();
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
        if (localReadBuffer != null && !localReadBuffer.isDirect() && lastLargeMessageTime < lastReadTime - 30 * 1000L) {  // used temp heap
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("change to direct con read buffer ,cur temp buf size :" + localReadBuffer.capacity());
            }
            recycle(localReadBuffer);
            netReadBuffer = allocate(readBufferChunk, generateBufferRecordBuilder().withType(BufferType.POOL));
        } else {
            if (localReadBuffer != null) {
                IODelayProvider.inReadReachEnd();
                localReadBuffer.clear();
            }
        }
    }

    private void processSSLAppData(byte[] packetData) throws IOException {
        if (packetData == null) return;
        sslHandler.unwrapAppData(packetData);
        handleNonSSL(getBottomReadBuffer());
    }

    public void processSSLPacketNotBigEnough(ByteBuffer buffer, int offset, final int pkgLength) {
        ByteBuffer newBuffer = allocate(pkgLength, generateBufferRecordBuilder().withType(BufferType.POOL));
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
        if (isUseSSL()) sslHandler.close();
        super.close(reason);
    }

    @Override
    public synchronized void recycleReadBuffer() {
        recycleNetReadBuffer();
        super.recycleReadBuffer();
    }

    private void recycleNetReadBuffer() {
        if (this.netReadBuffer != null) {
            this.recycle(this.netReadBuffer);
            this.netReadBuffer = null;
        }
    }

    @Override
    public void startFlowControl(int currentWritingSize) {
        if (!frontWriteFlowControlled && this.getService() instanceof BusinessService && currentWritingSize > FlowController.getFlowHighLevel()) {
            ((BusinessService) this.getService()).getSession().startFlowControl(currentWritingSize);
        }
    }

    @Override
    public void stopFlowControl(int currentWritingSize) {
        if (this.getService() instanceof BusinessService && currentWritingSize <= FlowController.getFlowLowLevel()) {
            ((BusinessService) this.getService()).getSession().stopFlowControl(currentWritingSize);
        }
    }

    @Override
    public void cleanup(String reason) {
        if (isCleanUp.compareAndSet(false, true)) {
            recycleNetReadBuffer();
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
        if (!isUseSSL()) return orgBuffer;
        return sslHandler.wrapAppData(orgBuffer);
    }

    @Override
    public void compactReadBuffer(ByteBuffer dataBuffer, int offset, boolean isSSL) throws IOException {
        if (dataBuffer == null) {
            return;
        }
        if (isSupportSSL && isSSL) {
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
            setBottomReadBuffer(dataBuffer.compact());
        }
    }


    public ByteBuffer ensureReadBufferFree(ByteBuffer oldBuffer, int expectSize) {
        ByteBuffer newBuffer = allocate(expectSize < 0 ? processor.getBufferPool().getChunkSize() : expectSize, generateBufferRecordBuilder().withType(BufferType.POOL));
        oldBuffer.flip();
        newBuffer.put(oldBuffer);
        setBottomReadBuffer(newBuffer);

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

    @Override
    public ByteBuffer findReadBuffer() {
        if (isSupportSSL && maybeUseSSL()) {
            if (this.netReadBuffer == null) {
                netReadBuffer = allocate(processor.getBufferPool().getChunkSize(), generateBufferRecordBuilder().withType(BufferType.POOL));
            }
            return netReadBuffer;
        } else {
            //only recycle this read buffer
            recycleNetReadBuffer();
            return super.findReadBuffer();
        }
    }

    @Override
    ByteBuffer getReadBuffer() {
        if (isSupportSSL && maybeUseSSL()) {
            return netReadBuffer;
        } else {
            return super.getReadBuffer();
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
        return "FrontendConnection[id = " + id + " port = " + port + " host = " + host + " local_port = " + localPort + " isManager = " + isManager() + " startupTime = " + startupTime + " skipCheck = " + isSkipCheck() + " isFlowControl = " + isFrontWriteFlowControlled() + " onlyTcpConnect = " + isOnlyFrontTcpConnected() + " ssl = " + (isUseSSL() ? sslName : "no") + "]";
    }

    public String getSimple() {
        StringBuilder s = new StringBuilder();
        s.append("id:");
        s.append(id);
        s.append("/");
        s.append(((FrontendService) getService()).getUser().getFullName());
        s.append("@");
        s.append(host);
        s.append(":");
        s.append(localPort);
        return s.toString();
    }
}
