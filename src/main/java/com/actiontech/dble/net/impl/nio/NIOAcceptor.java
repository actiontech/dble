/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net.impl.nio;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketAcceptor;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.factory.FrontendConnectionFactory;
import com.actiontech.dble.util.SelectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.*;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author mycat
 */
public final class NIOAcceptor extends Thread implements SocketAcceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOAcceptor.class);
    private static final AcceptIdGenerator ID_GENERATOR = new AcceptIdGenerator();

    private final int port;
    private Selector selector;
    private final ServerSocketChannel serverChannel;
    private final FrontendConnectionFactory factory;
    private ConcurrentLinkedQueue<AbstractConnection> frontRegisterQueue;

    public NIOAcceptor(String name, String bindIp, int port, int backlog, FrontendConnectionFactory factory,
                       ConcurrentLinkedQueue<AbstractConnection> frontRegisterQueue) throws IOException {
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
        this.frontRegisterQueue = frontRegisterQueue;
    }

    public int getPort() {
        return port;
    }

    @Override
    public void run() {
        Selector tSelector = this.selector;
        int selectReturnsImmediately = 0;
        // use 80% of the timeout for measure
        long minSelectTimeout = TimeUnit.MILLISECONDS.toNanos(SelectorUtil.DEFAULT_SELECT_TIMEOUT) / 100 * 80;
        for (; ; ) {
            try {
                long beforeSelect = System.nanoTime();
                int selected = tSelector.select(SelectorUtil.DEFAULT_SELECT_TIMEOUT);

                //issue - https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6403933
                //refer to https://github.com/netty/netty/pull/565
                if (selected == 0) {
                    long timeBlocked = System.nanoTime() - beforeSelect;
                    boolean checkSelectReturnsImmediately = SelectorUtil.checkSelectReturnsImmediately(timeBlocked, tSelector, minSelectTimeout);
                    if (checkSelectReturnsImmediately) {
                        selectReturnsImmediately++;
                    } else {
                        selectReturnsImmediately = 0;
                    }
                    if (selectReturnsImmediately == 1024) {
                        // The selector returned immediately for 10 times in a row,
                        // so recreate one selector as it seems like we hit the
                        // famous epoll(..) jdk bug.
                        Selector rebuildSelector = SelectorUtil.rebuildSelector(this.selector);
                        if (null != rebuildSelector) {
                            this.selector = rebuildSelector;
                        }
                        tSelector = this.selector;
                        selectReturnsImmediately = 0;
                        // try to select again
                        continue;
                    }
                } else {
                    selectReturnsImmediately = 0;
                }

                Set<SelectionKey> keys = tSelector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        if (key.isValid() && key.isAcceptable()) {
                            accept();
                        } else {
                            key.cancel();
                        }
                    }
                } catch (final Throwable e) {
                    LOGGER.warn("caught Throwable err: ", e);
                } finally {
                    keys.clear();
                }
            } catch (Throwable e) {
                LOGGER.info(getName(), e);
            }
        }
    }

    private void accept() {
        SocketChannel channel = null;
        try {
            channel = serverChannel.accept();
            channel.configureBlocking(false);
            NIOSocketWR socketWR = new NIOSocketWR();
            FrontendConnection c = factory.make(channel, socketWR);
            socketWR.initFromConnection(c);
            c.setId(ID_GENERATOR.getId());
            IOProcessor processor = DbleServer.getInstance().nextFrontProcessor();
            c.setProcessor(processor);

            frontRegisterQueue.offer(c);
            wakeupFrontedSelector();

        } catch (Exception e) {
            //if this exception is "SocketException: Invalid argument." , maybe you are doing setOption when the frontendConnection was closed by peer. In most of those cases , you are using SLB/LVS on MACOS, just ignore it.
            LOGGER.info(getName(), e);
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
                LOGGER.info("closeChannelError", e);
            }
        }
        try {
            channel.close();
        } catch (IOException e) {
            LOGGER.info("closeChannelError", e);
        }
    }

    //wakeup selector
    private void wakeupFrontedSelector() {
        Map<Thread, Runnable> threadRunnableMap = DbleServer.getInstance().getRunnableMap().get(DbleServer.NIO_FRONT_RW);
        Map<Runnable, Integer> runnableMap = threadRunnableMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, entry -> ((RW) entry.getValue()).getSelectorKeySize()));
        Optional<Map.Entry<Runnable, Integer>> min = runnableMap.entrySet().stream().min(Map.Entry.comparingByValue());
        if (min.isPresent()) {
            ((RW) min.get().getKey()).wakeUpSelector();
        } else {
            LOGGER.warn("wakeupFrontedSelector error");
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
