package com.actiontech.dble.net.connection;

import com.actiontech.dble.backend.mysql.proto.handler.Impl.SSLProtoHandler;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.net.service.SSLProtoServerTask;
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

    private SSLHandler sslHandler;

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
    }

    @Override
    public void businessClose(String reason) {
        this.close(reason);
    }

    public void close(String reason) {
        if (isUseSSL())
            sslHandler.close();
        super.close(reason);
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
    public void handle(ByteBuffer dataBuffer) throws IOException {
        if (isUseSSL()) {
            handlerSSLReadBuffer(dataBuffer, 0);
        } else {
            super.handle(dataBuffer);
        }
    }

    @Override
    public void compactReadBuffer(ByteBuffer dataBuffer, int offset) throws IOException {
        handlerSSLReadBuffer(dataBuffer, offset);
    }

    public void initSSLHandShark() throws IOException {
        sslHandler = new SSLHandler(this);
        sslHandler.init();
    }

    public boolean doSSLHandShark(byte[] data) {
        try {
            if (!isUseSSL()) {
                ((FrontendService) getService()).writeErrMessage(ErrorCode.ER_HANDSHAKE_ERROR, "SSL not initialized");
                return false;
            }
            sslHandler.unwrapNonAppData(data);
            return sslHandler.isHandshakeSuccess();
        } catch (SSLException e) {
            LOGGER.error("SSL handshake failed, exception: {},", e);
            close("SSL handshake failed");
        }
        return false;
    }

    @Override
    public ByteBuffer wrap(ByteBuffer orgBuffer) throws SSLException {
        if (!isUseSSL())
            return orgBuffer;
        return sslHandler.wrapAppData(orgBuffer);
    }

    public ByteBuffer unwrap(byte[] packetData) throws SSLException {
        if (!isUseSSL()) {
            ByteBuffer buffer = allocate(packetData.length);
            buffer.put(packetData);
            return buffer;
        }
        return sslHandler.unwrapAppData(packetData);
    }

    private void handlerSSLReadBuffer(ByteBuffer dataBuffer, int offset) throws IOException {
        if (dataBuffer == null) {
            return;
        }
        SSLProtoHandler proto = new SSLProtoHandler();
        boolean hasRemaining = true;
        while (hasRemaining) {
            ProtoHandlerResult result = proto.handle(dataBuffer, offset, false);
            switch (result.getCode()) {
                case SSL_PROTO_PACKET:
                    if (!result.isHasMorePacket()) {
                        readReachEnd();
                    }
                    processSSLProto(result.getPacketData());
                    if (!result.isHasMorePacket()) {
                        dataBuffer.clear();
                    }
                    break;
                case SSL_APP_PACKET:
                    processSSLAppData(result, dataBuffer, offset);
                    break;
                case SSL_BUFFER_NOT_BIG_ENOUGH:
                    ensureFreeSpaceOfReadBuffer(dataBuffer, result.getOffset(), result.getPacketLength());
                    break;
                case BUFFER_PACKET_UNCOMPLETE:
                    compactReadBuffer0(dataBuffer, offset);
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

    private void processSSLProto(byte[] packetData) {
        final AbstractService frontService = getService();
        if (frontService == null) {
            LOGGER.warn("front connection{} has been closed,ignore packet.", this);
        } else if (packetData != null) {
            pushServiceTask(new SSLProtoServerTask(packetData, frontService));
        }
    }

    /**
     * replace 'ssl data' in the dataBuffer with 'mysql data'
     * the interval of 'ssl data' is [preOffset, result.getOffset())
     */
    private void processSSLAppData(ProtoHandlerResult result, ByteBuffer dataBuffer, int preOffset) throws IOException {
        final ByteBuffer tmpBuffer = unwrap(result.getPacketData());
        int orgPosition = dataBuffer.position();

        byte[] preData = new byte[preOffset];
        dataBuffer.get(preData);

        dataBuffer.position(result.getOffset());

        byte[] lastData = new byte[orgPosition - result.getOffset()];
        dataBuffer.get(lastData);

        tmpBuffer.flip();
        dataBuffer.clear();
        result.setOffset(preOffset + tmpBuffer.limit());

        int len = result.getOffset() + lastData.length;
        if (dataBuffer.capacity() < len) {
            ByteBuffer oldBuffer = dataBuffer;
            dataBuffer = allocate(len);
            this.readBuffer = dataBuffer;
            recycle(oldBuffer);
        }

        dataBuffer.put(preData);
        dataBuffer.put(tmpBuffer);
        dataBuffer.put(lastData);
        recycle(tmpBuffer);
        super.handle(dataBuffer);
    }

    private void compactReadBuffer0(ByteBuffer buffer, int offset) {
        if (buffer == null) {
            return;
        }
        buffer.limit(buffer.position());
        buffer.position(offset);
        this.readBuffer = buffer.compact();
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
