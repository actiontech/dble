/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.connection;

import com.actiontech.dble.buffer.BufferType;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.AuthService;
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
