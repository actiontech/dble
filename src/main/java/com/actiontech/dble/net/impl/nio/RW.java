package com.actiontech.dble.net.impl.nio;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.*;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class RW implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RW.class);
    private final Selector selector;
    private final ConcurrentLinkedQueue<AbstractConnection> registerQueue;
    private boolean useThreadUsageStat = false;
    private ThreadWorkUsage workUsage;

    public RW(ConcurrentLinkedQueue queue) throws IOException {
        this.selector = Selector.open();
        this.registerQueue = queue;
    }

    @Override
    public void run() {
        final Selector finalSelector = this.selector;
        Set<SelectionKey> keys = null;
        if (SystemConfig.getInstance().getUseThreadUsageStat() == 1) {
            this.useThreadUsageStat = true;
            this.workUsage = new ThreadWorkUsage();
            DbleServer.getInstance().getThreadUsedMap().put(Thread.currentThread().getName(), workUsage);
        }
        for (; ; ) {
            try {
                if (Thread.currentThread().isInterrupted()) {
                    DbleServer.getInstance().getThreadUsedMap().remove(Thread.currentThread().getName());
                    finalSelector.close();
                    LOGGER.debug("interrupt thread:{}", Thread.currentThread().toString());
                    break;
                }
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
            } catch (ClosedSelectorException e) {
                LOGGER.info(Thread.currentThread().getName(), e);
            } catch (Exception e) {
                LOGGER.info(Thread.currentThread().getName(), e);
            } catch (final Throwable e) {
                // Catch exceptions such as OOM so that the reactor can keep running!
                // @author Uncle-pan
                // @since 2016-03-30
                LOGGER.warn("caught Throwable err: ", e);
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
                    if (con.isClosed()) {
                        key.cancel();
                    }
                    if (key.isValid() && key.isReadable()) {
                        try {
                            con.asyncRead();
                        } catch (ClosedChannelException e) {
                            //happens when close and read running in parallel.
                            //sometimes ,no byte could be read,but an  read event triggered with  zero bytes although cause this.
                            LOGGER.info("read bytes but the  connection is closed .connection is {}. May be the connection closed suddenly.", con);
                            key.cancel();
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
                LOGGER.warn("caught err:" + con, e);
                AlertUtil.alertSelf(AlarmCode.NIOREACTOR_UNKNOWN_EXCEPTION, Alert.AlertLevel.WARN, "caught err:" + con + e.getMessage(), null);
            } catch (final Throwable e) {
                // Catch exceptions such as OOM and close connection if exists
                //so that the reactor can keep running!
                // @author Uncle-pan
                // @since 2016-03-30
                if (con != null) {
                    con.close("Bad: " + e);
                }
                LOGGER.warn("caught err: ", e);
                AlertUtil.alertSelf(AlarmCode.NIOREACTOR_UNKNOWN_THROWABLE, Alert.AlertLevel.WARN, "caught err:" + e.getMessage(), null);
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
                //todo 确认调用register的时候会发生什么
                LOGGER.warn("register err", e);
            }
        }
    }

    public Selector getSelector() {
        return selector;
    }

    public int getSelectorKeySize() {
        if (selector.isOpen()) {
            return selector.keys().size();
        }
        return 0;
    }
}
