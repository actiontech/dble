/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package com.actiontech.dble.net;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.factory.FrontendConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

/**
 * @author mycat
 */
public final class NIOAcceptor extends Thread implements SocketAcceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOAcceptor.class);
    private static final AcceptIdGenerator ID_GENERATOR = new AcceptIdGenerator();

    private final int port;
    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final FrontendConnectionFactory factory;
    private final NIOReactorPool reactorPool;

    public NIOAcceptor(String name, String bindIp, int port, int backlog, FrontendConnectionFactory factory,
                       NIOReactorPool reactorPool) throws IOException {
        super.setName(name);
        this.port = port;
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        //set TCP option
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 16 * 2);
        serverChannel.bind(new InetSocketAddress(bindIp, port), backlog);
        this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        this.factory = factory;
        this.reactorPool = reactorPool;
    }

    public int getPort() {
        return port;
    }

    @Override
    public void run() {
        final Selector tSelector = this.selector;
        for (; ; ) {
            try {
                tSelector.select(1000L);
                Set<SelectionKey> keys = tSelector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        if (key.isValid() && key.isAcceptable()) {
                            accept();
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
            } catch (Exception e) {
                LOGGER.warn(getName(), e);
            }
        }
    }

    private void accept() {
        SocketChannel channel = null;
        try {
            channel = serverChannel.accept();
            channel.configureBlocking(false);
            FrontendConnection c = factory.make(channel);
            c.setAccepted(true);
            c.setId(ID_GENERATOR.getId());
            NIOProcessor processor = DbleServer.getInstance().nextProcessor();
            c.setProcessor(processor);

            NIOReactor reactor = reactorPool.getNextReactor();
            reactor.postRegister(c);

        } catch (Exception e) {
            LOGGER.warn(getName(), e);
            closeChannel(channel);
        }
    }

    private static void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        Socket socket = channel.socket();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                LOGGER.error("closeChannelError", e);
            }
        }
        try {
            channel.close();
        } catch (IOException e) {
            LOGGER.error("closeChannelError", e);
        }
    }

    /**
     * @author mycat
     */
    private static class AcceptIdGenerator {

        private static final long MAX_VALUE = 0xffffffffL;

        private long acceptId = 0L;
        private final Object lock = new Object();

        private long getId() {
            synchronized (lock) {
                if (acceptId >= MAX_VALUE) {
                    acceptId = 0L;
                }
                return ++acceptId;
            }
        }
    }

}
