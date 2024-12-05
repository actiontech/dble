/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.net.impl.aio;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.SocketConnector;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.AuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mycat
 */
public final class AIOConnector implements SocketConnector,
        CompletionHandler<Void, AbstractService> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AIOConnector.class);
    private static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

    public AIOConnector() {

    }

    @Override
    public void completed(Void result, AbstractService attachment) {
        finishConnect(attachment);
    }

    @Override
    public void failed(Throwable exc, AbstractService service) {
        service.getConnection().onConnectFailed(exc);
    }

    private void finishConnect(AbstractService service) {
        try {
            if (service.getConnection().finishConnect()) {
                service.getConnection().setId(ID_GENERATOR.getId());
                IOProcessor processor = OBsharding_DServer.getInstance().nextBackendProcessor();
                service.getConnection().setProcessor(processor);
                if (service instanceof AuthService) {
                    ((AuthService) service).register();
                }
            }
        } catch (Exception e) {
            LOGGER.info("connect err ", e);
            service.getConnection().close(e.toString());
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void postConnect(AbstractConnection c) {
        ((AsynchronousSocketChannel) c.getChannel()).connect(new InetSocketAddress(c.getHost(), c.getPort()), c.getService(), this);
    }

    /**
     * @author mycat
     */
    private static class ConnectIdGenerator {

        private AtomicLong connectId = new AtomicLong(0);

        private long getId() {
            return connectId.incrementAndGet();
        }
    }
}
