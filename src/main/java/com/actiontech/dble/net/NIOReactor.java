/*
* Copyright (C) 2016-2018 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.net;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.log.alarm.AlarmCode;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
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
    private ThreadWorkUsage workUsage;
    private boolean useThreadUsageStat = false;

    public NIOReactor(String name) throws IOException {
        this.name = name;
        this.reactorR = new RW();
        if (DbleServer.getInstance().getConfig().getSystem().getUseThreadUsageStat() == 1) {
            this.useThreadUsageStat = true;
            this.workUsage = new ThreadWorkUsage();
            DbleServer.getInstance().getThreadUsedMap().put(name, workUsage);
        }
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
                    long workStart = 0;
                    if (useThreadUsageStat) {
                        workStart = System.nanoTime();
                    }
                    finalSelector.select(500L);
                    if (useThreadUsageStat) {
                        long afterSelect = System.nanoTime();
                        if (afterSelect - workStart > 3000000) { // 3ms
                            workStart = afterSelect;
                        }
                    }
                    register(finalSelector);
                    keys = finalSelector.selectedKeys();
                    if (keys.size() == 0) {
                        continue;
                    }
                    executeKeys(keys);
                    if (useThreadUsageStat) {
                        workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
                    }
                } catch (Exception e) {
                    LOGGER.info(name, e);
                } catch (final Throwable e) {
                    // Catch exceptions such as OOM so that the reactor can keep running!
                    // @author Uncle-pan
                    // @since 2016-03-30
                    LOGGER.info("caught err: ", e);
                } finally {
                    if (keys != null) {
                        keys.clear();
                    }

                }
            }
        }

        private void executeKeys(Set<SelectionKey> keys) {
            for (SelectionKey key : keys) {
                AbstractConnection con = null;
                try {
                    Object att = key.attachment();
                    if (att != null) {
                        con = (AbstractConnection) att;
                        if (key.isValid() && key.isReadable()) {
                            try {
                                con.asyncRead();
                            } catch (IOException e) {
                                con.close("program err:" + e.toString());
                                continue;
                            } catch (Exception e) {
                                LOGGER.warn(AlarmCode.CORE_GENERAL_WARN + "caught err:", e);
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
                    LOGGER.warn(AlarmCode.CORE_GENERAL_WARN + con + " " + e);
                } catch (final Throwable e) {
                    // Catch exceptions such as OOM and close connection if exists
                    //so that the reactor can keep running!
                    // @author Uncle-pan
                    // @since 2016-03-30
                    if (con != null) {
                        con.close("Bad: " + e);
                    }
                    LOGGER.warn(AlarmCode.CORE_GENERAL_WARN + "caught err: ", e);
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
                    LOGGER.warn(AlarmCode.CORE_GENERAL_WARN + "register err", e);
                }
            }
        }

    }

}
