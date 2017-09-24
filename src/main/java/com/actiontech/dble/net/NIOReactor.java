/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * NIOReactor
 * <p>
 * <p>
 * Catch exceptions such as OOM so that the reactor can keep running for response client!
 * </p>
 *
 * @author mycat, Uncle-pan
 * @since 2016-03-30
 */
public final class NIOReactor {
    private static final Logger LOGGER = LoggerFactory.getLogger(NIOReactor.class);
    private final String name;
    private final RW reactorR;

    public NIOReactor(String name) throws IOException {
        this.name = name;
        this.reactorR = new RW();
    }

    void startup() {
        new Thread(reactorR, name + "-RW").start();
    }

    void postRegister(AbstractConnection c) {
        reactorR.registerQueue.offer(c);
        reactorR.selector.wakeup();
    }

    private final class RW implements Runnable {
        private final Selector selector;
        private final ConcurrentLinkedQueue<AbstractConnection> registerQueue;

        private RW() throws IOException {
            this.selector = Selector.open();
            this.registerQueue = new ConcurrentLinkedQueue<>();
        }

        @Override
        public void run() {
            final Selector finalSelector = this.selector;
            Set<SelectionKey> keys = null;
            for (; ; ) {
                try {
                    finalSelector.select(500L);
                    register(finalSelector);
                    keys = finalSelector.selectedKeys();
                    for (SelectionKey key : keys) {
                        AbstractConnection con = null;
                        try {
                            Object att = key.attachment();
                            if (att != null) {
                                con = (AbstractConnection) att;
                                if (key.isValid() && key.isReadable()) {
                                    try {
                                        con.asynRead();
                                    } catch (IOException e) {
                                        con.close("program err:" + e.toString());
                                        continue;
                                    } catch (Exception e) {
                                        LOGGER.warn("caught err:", e);
                                        con.close("program err:" + e.toString());
                                        continue;
                                    }
                                }
                                if (key.isValid() && key.isWritable()) {
                                    con.doNextWriteCheck();
                                }
                            } else {
                                key.cancel();
                            }
                        } catch (CancelledKeyException e) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(con + " socket key canceled");
                            }
                        } catch (Exception e) {
                            LOGGER.warn(con + " " + e);
                        } catch (final Throwable e) {
                            // Catch exceptions such as OOM and close connection if exists
                            //so that the reactor can keep running!
                            // @author Uncle-pan
                            // @since 2016-03-30
                            if (con != null) {
                                con.close("Bad: " + e);
                            }
                            LOGGER.error("caught err: ", e);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn(name, e);
                } catch (final Throwable e) {
                    // Catch exceptions such as OOM so that the reactor can keep running!
                    // @author Uncle-pan
                    // @since 2016-03-30
                    LOGGER.error("caught err: ", e);
                } finally {
                    if (keys != null) {
                        keys.clear();
                    }

                }
            }
        }

        private void register(Selector finalSelector) {
            AbstractConnection c;
            if (registerQueue.isEmpty()) {
                return;
            }
            while ((c = registerQueue.poll()) != null) {
                try {
                    ((NIOSocketWR) c.getSocketWR()).register(finalSelector);
                    c.register();
                } catch (Exception e) {
                    c.close("register err" + e.toString());
                    LOGGER.error("register err", e);
                }
            }
        }

    }

}
