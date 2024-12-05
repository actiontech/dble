/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.*;
import java.util.ConcurrentModificationException;

public final class SelectorUtil {

    private SelectorUtil() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectorUtil.class);

    public static final long DEFAULT_SELECT_TIMEOUT_RW = 500;
    public static final long DEFAULT_SELECT_TIMEOUT = 1000;


    public static boolean checkSelectReturnsImmediately(long timeBlocked, Selector finalSelector, long minSelectTimeout) {
        boolean selectReturnsImmediately = false;
        if (timeBlocked < minSelectTimeout) {
            // loop over all keys as the selector may was unblocked because of a closed channel
            boolean notConnected = false;

            for (SelectionKey key : finalSelector.keys()) {
                SelectableChannel ch = key.channel();
                try {
                    if (!ch.isOpen() || ch instanceof SocketChannel && !((SocketChannel) ch).isConnected() && !((SocketChannel) ch).isConnectionPending()) {
                        notConnected = true;
                        // cancel the key just to be on the safe side
                        key.cancel();
                    }
                } catch (CancelledKeyException e) {
                    // ignore
                    LOGGER.debug("CancelledKeyException ignore");
                }
            }

            if (!notConnected) {
                // returned before the minSelectTimeout elapsed with nothing select.
                // this may be the cause of the jdk epoll(..) bug, so increment the counter
                // which we use later to see if its really the jdk bug.
                selectReturnsImmediately = true;
            }
        }
        return selectReturnsImmediately;
    }

    public static Selector rebuildSelector(Selector oldSelector) {
        final Selector newSelector;

        if (oldSelector == null) {
            return null;
        }

        try {
            newSelector = Selector.open();
        } catch (Exception e) {
            LOGGER.warn("Failed to create a new Selector.", e);
            return null;
        }

        // Register all channels to the new Selector.
        int nChannels = 0;

        for (; ; ) {
            try {
                for (SelectionKey key : oldSelector.keys()) {
                    try {
                        if (key.channel().keyFor(newSelector) != null) {
                            continue;
                        }

                        int interestOps = key.interestOps();
                        key.cancel();
                        key.channel().register(newSelector, interestOps, key.attachment());
                        nChannels++;
                    } catch (Exception e) {
                        LOGGER.warn("Failed to re-register a Channel to the new Selector,", e);
                    }
                }
            } catch (ConcurrentModificationException e) {
                // Probably due to concurrent modification of the key set.
                continue;
            }
            break;
        }

        try {
            // time to close the old selector as everything else is registered to the new one
            oldSelector.close();
        } catch (Throwable t) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Failed to close the old Selector.", t);
            }
        }

        LOGGER.info("Migrated " + nChannels + " channel(s) to the new Selector,");
        return newSelector;
    }
}
