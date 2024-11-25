/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.net.impl.nio;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.alarm.AlarmCode;
import com.oceanbase.obsharding_d.alarm.Alert;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.SocketConnector;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.util.SelectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author mycat
 */
public final class NIOConnector extends Thread implements SocketConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOConnector.class);
    public static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

    private final String name;
    private Selector selector;
    private final BlockingQueue<AbstractConnection> connectQueue;
    private ConcurrentLinkedQueue<AbstractConnection> backendRegisterQueue;
    protected final AtomicBoolean wakenUp = new AtomicBoolean();

    public NIOConnector(String name, ConcurrentLinkedQueue<AbstractConnection> backendRegisterQueue)
            throws IOException {
        super.setName(name);
        this.name = name;
        this.selector = Selector.open();
        this.backendRegisterQueue = backendRegisterQueue;
        this.connectQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void postConnect(AbstractConnection c) {
        connectQueue.offer(c);
        if (wakenUp.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    @Override
    public void run() {
        Selector tSelector = this.selector;
        int selectReturnsImmediately = 0;
        boolean wakenupFromLoop = false;
        // use 80% of the timeout for measure
        long minSelectTimeout = TimeUnit.MILLISECONDS.toNanos(SelectorUtil.DEFAULT_SELECT_TIMEOUT) / 100 * 80;
        for (; ; ) {
            try {
                wakenUp.set(false);
                long beforeSelect = System.nanoTime();
                int selected = tSelector.select(SelectorUtil.DEFAULT_SELECT_TIMEOUT);

                //issue - https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6403933
                //refer to https://github.com/netty/netty/pull/565
                if (selected == 0 && !wakenupFromLoop && !wakenUp.get()) {
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
                        wakenupFromLoop = false;
                        // try to select again
                        continue;
                    }
                } else {
                    selectReturnsImmediately = 0;
                }

                if (wakenUp.get()) {
                    wakenupFromLoop = true;
                    tSelector.wakeup();
                } else {
                    wakenupFromLoop = false;
                }

                connect(tSelector);
                Set<SelectionKey> keys = tSelector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        Object att = key.attachment();
                        if (att != null && key.isValid() && key.isConnectable()) {
                            finishConnect(key, att);
                        } else {
                            key.cancel();
                        }
                    }
                } catch (final Throwable e) {
                    LOGGER.warn("caught Throwable err: ", e);
                } finally {
                    if (keys != null) {
                        keys.clear();
                    }
                }
            } catch (Exception e) {
                LOGGER.warn(name, e);
                AlertUtil.alertSelf(AlarmCode.NIOCONNECTOR_UNKNOWN_EXCEPTION, Alert.AlertLevel.WARN, name + e.getMessage(), null);
            }
        }
    }

    private void connect(Selector finalSelector) {
        AbstractConnection c;
        while ((c = connectQueue.poll()) != null) {
            try {
                SocketChannel channel = (SocketChannel) c.getChannel();
                channel.register(finalSelector, SelectionKey.OP_CONNECT, c);
                channel.connect(new InetSocketAddress(c.getHost(), c.getPort()));
            } catch (Exception e) {
                LOGGER.warn("error:", e);
                c.onConnectFailed(e);
            }
        }
    }

    private void finishConnect(SelectionKey key, Object att) {
        BackendConnection c = (BackendConnection) att;
        try {
            if (finishConnect(c, (SocketChannel) c.getChannel())) {
                clearSelectionKey(key);
                c.setId(ID_GENERATOR.getId());
                IOProcessor processor = OBsharding_DServer.getInstance().nextBackendProcessor();
                c.setProcessor(processor);
                backendRegisterQueue.offer(c);
                wakeupBackendSelector();
            }
        } catch (Exception e) {
            clearSelectionKey(key);
            LOGGER.warn("connect error,the channel  is {}", c, e);
            c.close(e.toString());
        }
    }

    private boolean finishConnect(AbstractConnection c, SocketChannel channel)
            throws IOException {
        if (channel.isConnectionPending()) {
            channel.finishConnect();
            c.setLocalPort(channel.socket().getLocalPort());
            return true;
        } else {
            return false;
        }
    }

    private void clearSelectionKey(SelectionKey key) {
        if (key.isValid()) {
            key.attach(null);
            key.cancel();
        }
    }

    //wakeup selector
    private void wakeupBackendSelector() {
        Map<Thread, Runnable> threadRunnableMap = OBsharding_DServer.getInstance().getRunnableMap().get(OBsharding_DServer.NIO_BACKEND_RW);
        Map<Runnable, Integer> runnableMap = threadRunnableMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, entry -> ((RW) entry.getValue()).getSelectorKeySize()));
        Optional<Map.Entry<Runnable, Integer>> min = runnableMap.entrySet().stream().min(Map.Entry.comparingByValue());
        if (min.isPresent()) {
            ((RW) min.get().getKey()).wakeUpSelector();
        } else {
            LOGGER.warn("wakeupBackendSelector error");
        }
    }

    /**
     * @author mycat
     */
    public static class ConnectIdGenerator {
        private AtomicLong connectId = new AtomicLong(0);

        public long getId() {
            return connectId.incrementAndGet();
        }
    }

}
