/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net;

import com.actiontech.dble.DbleServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mycat
 */
public final class AIOConnector implements SocketConnector,
        CompletionHandler<Void, BackendAIOConnection> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AIOConnector.class);
    private static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

    public AIOConnector() {

    }

    @Override
    public void completed(Void result, BackendAIOConnection attachment) {
        finishConnect(attachment);
    }

    @Override
    public void failed(Throwable exc, BackendAIOConnection conn) {
        conn.onConnectFailed(exc);
    }

    private void finishConnect(BackendAIOConnection c) {
        try {
            if (c.finishConnect()) {
                c.setId(ID_GENERATOR.getId());
                NIOProcessor processor = DbleServer.getInstance().nextProcessor();
                c.setProcessor(processor);
                c.register();
            }
        } catch (Exception e) {
            c.onConnectFailed(e);
            LOGGER.info("connect err ", e);
            c.close(e.toString());
        }
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
