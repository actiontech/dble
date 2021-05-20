/*
* Copyright (C) 2016-2021 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net.impl.nio;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketConnector;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mycat
 */
public final class NIOConnector extends Thread implements SocketConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOConnector.class);
    public static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

    private final String name;
    private final Selector selector;
    private final BlockingQueue<AbstractConnection> connectQueue;
    private ConcurrentLinkedQueue<AbstractConnection> backendRegisterQueue;

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
        selector.wakeup();
    }

    @Override
    public void run() {
        final Selector tSelector = this.selector;
        for (; ; ) {
            try {
                tSelector.select(1000L);
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
                IOProcessor processor = DbleServer.getInstance().nextBackendProcessor();
                c.setProcessor(processor);
                backendRegisterQueue.offer(c);
                wakeupBackendSelector();
            }
        } catch (Exception e) {
            clearSelectionKey(key);
            LOGGER.warn("connect error,the channel  is {}", c, e);
            c.close(e.toString());
            c.onConnectFailed(e);

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
        Map<Thread, Runnable> threadRunnableMap = DbleServer.getInstance().getRunnableMap().get(DbleServer.BACKEND_EXECUTOR_NAME);
        for (Map.Entry<Thread, Runnable> runnableEntry : threadRunnableMap.entrySet()) {
            RW rw = (RW) runnableEntry.getValue();
            rw.getSelector().wakeup();
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
