/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.factory.FrontendConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mycat
 */
public final class AIOAcceptor implements SocketAcceptor,
        CompletionHandler<AsynchronousSocketChannel, Long> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AIOAcceptor.class);
    private static final AcceptIdGenerator ID_GENERATOR = new AcceptIdGenerator();

    private final int port;
    private final AsynchronousServerSocketChannel serverChannel;
    private final FrontendConnectionFactory factory;

    private final String name;

    public AIOAcceptor(String name, String ip, int port, int backlog,
                       FrontendConnectionFactory factory, AsynchronousChannelGroup group)
            throws IOException {
        this.name = name;
        this.port = port;
        this.factory = factory;
        serverChannel = AsynchronousServerSocketChannel.open(group);
        /** set TCP option */
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 16 * 2);
        // backlog=100
        serverChannel.bind(new InetSocketAddress(ip, port), backlog);
    }

    public String getName() {
        return name;
    }

    public void start() {
        this.pendingAccept();
    }

    public int getPort() {
        return port;
    }

    private void accept(NetworkChannel channel, Long id) {
        try {
            FrontendConnection c = factory.make(channel);
            c.setAccepted(true);
            c.setId(id);
            NIOProcessor processor = DbleServer.getInstance().nextProcessor();
            c.setProcessor(processor);
            c.register();
        } catch (Exception e) {
            LOGGER.error("AioAcceptorError", e);
            closeChannel(channel);
        }
    }

    private void pendingAccept() {
        if (serverChannel.isOpen()) {
            serverChannel.accept(ID_GENERATOR.getId(), this);
        } else {
            throw new IllegalStateException(
                    "Server Channel has been closed");
        }

    }

    @Override
    public void completed(AsynchronousSocketChannel result, Long id) {
        accept(result, id);
        // next pending waiting
        pendingAccept();

    }

    @Override
    public void failed(Throwable exc, Long id) {
        LOGGER.info("acception connect failed:" + exc);
        // next pending waiting
        pendingAccept();

    }

    private static void closeChannel(NetworkChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException e) {
            LOGGER.error("AioAcceptorError", e);
        }
    }

    /**
     * @author mycat
     */
    private static class AcceptIdGenerator {

        private static final long MAX_VALUE = 0xffffffffL;

        private AtomicLong acceptId = new AtomicLong();
        private final Object lock = new Object();

        private long getId() {
            long newValue = acceptId.getAndIncrement();
            if (newValue >= MAX_VALUE) {
                synchronized (lock) {
                    newValue = acceptId.getAndIncrement();
                    if (newValue >= MAX_VALUE) {
                        acceptId.set(0);
                    }
                }
                return acceptId.getAndDecrement();
            } else {
                return newValue;
            }
        }
    }
}
