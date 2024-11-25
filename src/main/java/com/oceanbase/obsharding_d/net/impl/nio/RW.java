/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.impl.nio;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.alarm.AlarmCode;
import com.oceanbase.obsharding_d.alarm.Alert;
import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.singleton.ConnectionAssociateThreadManager;
import com.oceanbase.obsharding_d.statistic.stat.ThreadWorkUsage;
import com.oceanbase.obsharding_d.util.SelectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.*;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RW implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RW.class);
    private Selector selector;
    private final ConcurrentLinkedQueue<AbstractConnection> registerQueue;
    private boolean useThreadUsageStat = false;
    private ThreadWorkUsage workUsage;

    protected final AtomicBoolean wakenUp = new AtomicBoolean();

    public RW(ConcurrentLinkedQueue queue) throws IOException {
        this.selector = Selector.open();
        this.registerQueue = queue;
    }

    @Override
    public void run() {
        Selector finalSelector = this.selector;
        Set<SelectionKey> keys = null;
        if (SystemConfig.getInstance().getUseThreadUsageStat() == 1) {
            this.useThreadUsageStat = true;
            this.workUsage = new ThreadWorkUsage();
            OBsharding_DServer.getInstance().getThreadUsedMap().put(Thread.currentThread().getName(), workUsage);
        }
        int selectReturnsImmediately = 0;
        boolean wakenupFromLoop = false;
        // use 80% of the timeout for measure
        long minSelectTimeout = TimeUnit.MILLISECONDS.toNanos(SelectorUtil.DEFAULT_SELECT_TIMEOUT_RW) / 100 * 80;
        for (; ; ) {
            try {
                wakenUp.set(false);
                if (Thread.currentThread().isInterrupted()) {
                    OBsharding_DServer.getInstance().getThreadUsedMap().remove(Thread.currentThread().getName());
                    finalSelector.close();
                    LOGGER.debug("interrupt thread:{}", Thread.currentThread().toString());
                    break;
                }
                long workStart = 0;
                if (useThreadUsageStat) {
                    workStart = System.nanoTime();
                }
                long beforeSelect = System.nanoTime();
                int selected = finalSelector.select(SelectorUtil.DEFAULT_SELECT_TIMEOUT_RW);

                //issue - https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6403933
                //refer to https://github.com/netty/netty/pull/565
                if (selected == 0 && !wakenupFromLoop && !wakenUp.get()) {
                    long timeBlocked = System.nanoTime() - beforeSelect;
                    boolean checkSelectReturnsImmediately = SelectorUtil.checkSelectReturnsImmediately(timeBlocked, finalSelector, minSelectTimeout);
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
                        finalSelector = this.selector;
                        selectReturnsImmediately = 0;
                        wakenupFromLoop = false;
                        // try to select again
                        continue;
                    }
                } else {
                    selectReturnsImmediately = 0;
                }
                // 'wakenUp.compareAndSet(false, true)' is always evaluated
                // before calling 'selector.wakeup()' to reduce the wake-up
                // overhead. (Selector.wakeup() is an expensive operation.)
                //
                // However, there is a race condition in this approach.
                // The race condition is triggered when 'wakenUp' is set to
                // true too early.
                //
                // 'wakenUp' is set to true too early if:
                // 1) Selector is waken up between 'wakenUp.set(false)' and
                //    'selector.select(...)'. (BAD)
                // 2) Selector is waken up between 'selector.select(...)' and
                //    'if (wakenUp.get()) { ... }'. (OK)
                //
                // In the first case, 'wakenUp' is set to true and the
                // following 'selector.select(...)' will wake up immediately.
                // Until 'wakenUp' is set to false again in the next round,
                // 'wakenUp.compareAndSet(false, true)' will fail, and therefore
                // any attempt to wake up the Selector will fail, too, causing
                // the following 'selector.select(...)' call to block
                // unnecessarily.
                //
                // To fix this problem, we wake up the selector again if wakenUp
                // is true immediately after selector.select(...).
                // It is inefficient in that it wakes up the selector for both
                // the first case (BAD - wake-up required) and the second case
                // (OK - no wake-up required).
                if (wakenUp.get()) {
                    wakenupFromLoop = true;
                    finalSelector.wakeup();
                } else {
                    wakenupFromLoop = false;
                }

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
                    try {
                        ConnectionAssociateThreadManager.getInstance().put(con);
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
                                if ((con.isOnlyFrontTcpConnected() && e instanceof IOException)) {
                                    LOGGER.debug("caught err:", e);
                                    con.close("connection was closed before receiving any data. May be just a heartbeat from SLB/LVS. detail: [" + e.toString() + "]");
                                } else {
                                    LOGGER.warn("caught err:", e);
                                    con.close("program err:" + e.toString());
                                }
                                continue;
                            }
                        }
                        if (key.isValid() && key.isWritable()) {
                            con.doNextWriteCheck();
                        }
                    } finally {
                        ConnectionAssociateThreadManager.getInstance().remove(con);
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
        c = registerQueue.poll();
        if (c == null) {
            return;
        }
        try {
            ConnectionAssociateThreadManager.getInstance().put(c);
            ((NIOSocketWR) c.getSocketWR()).register(finalSelector);
            c.register();
        } catch (Exception e) {
            //todo 确认调用register的时候会发生什么
            if ((c.isOnlyFrontTcpConnected() && e instanceof IOException)) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("{} register err", c, e);
            } else {
                LOGGER.warn("{} register err", c, e);
            }
        } finally {
            ConnectionAssociateThreadManager.getInstance().remove(c);
        }
    }

    public Selector getSelector() {
        return selector;
    }

    public void wakeUpSelector() {
        if (wakenUp.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    public int getSelectorKeySize() {
        if (selector.isOpen()) {
            return selector.keys().size();
        }
        return 0;
    }
}
