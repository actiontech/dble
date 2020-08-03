package com.actiontech.dble.net.connection;

import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.net.service.FrontEndService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.TimeUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

/**
 * Created by szf on 2020/6/23.
 */
public class FrontendConnection extends AbstractConnection {

    private static final long AUTH_TIMEOUT = 15 * 1000L;
    private final boolean isManager;

    protected final long idleTimeout = PoolConfig.DEFAULT_IDLE_TIMEOUT;

    public FrontendConnection(NetworkChannel channel, SocketWR socketWR, boolean isManager) throws IOException {
        super(channel, socketWR);
        this.isManager = isManager;
        InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
        InetSocketAddress remoteAddress = null;
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
    }

    @Override
    public void businessClose(String reason) {
        this.close(reason);
    }

    @Override
    public void setConnProperties(AuthResultInfo info) {

    }

    @Override
    public void startFlowControl() {
        ((ShardingService) this.getService()).getSession2().startFlowControl();
    }

    @Override
    public void stopFlowControl() {
        ((ShardingService) this.getService()).getSession2().stopFlowControl();
    }

    public synchronized void cleanup() {
        super.cleanup();
        if (getService() instanceof FrontEndService) {
            ((FrontEndService) getService()).userConnectionCount();
        }
    }

    @Override
    public void setProcessor(IOProcessor processor) {
        this.processor = processor;
        processor.addFrontend(this);
    }

    public boolean isIdleTimeout() {
        if (!(getService() instanceof AuthService)) {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + AUTH_TIMEOUT;
        }
    }


    public boolean isManager() {
        return isManager;
    }

    public FrontEndService getFrontEndService() {
        return (FrontEndService) getService();
    }
}
