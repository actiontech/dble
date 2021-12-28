package com.actiontech.dble.net.connection;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.services.BusinessService;
import com.actiontech.dble.services.FrontendService;
import com.actiontech.dble.util.TimeUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
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
    public void businessClose(String reason) {
        this.close(reason);
    }

    @Override
    public void startFlowControl() {
        ((BusinessService) this.getService()).getSession().startFlowControl();
    }

    @Override
    public void stopFlowControl() {
        ((BusinessService) this.getService()).getSession().stopFlowControl();
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
        return !(getService() instanceof AuthService);
    }

    public boolean isSkipCheck() {
        return skipCheck;
    }

    public void setSkipCheck(boolean skipCheck) {
        updateLastReadTime();
        this.skipCheck = skipCheck;
    }

    public String toString() {
        return "FrontendConnection[id = " + id + " port = " + port + " host = " + host + " local_port = " + localPort + " isManager = " + isManager() + " startupTime = " + startupTime + "skipCheck = " + isSkipCheck() + "]";
    }
}
