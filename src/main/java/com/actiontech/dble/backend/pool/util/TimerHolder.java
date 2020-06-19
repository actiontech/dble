package com.actiontech.dble.backend.pool.util;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import net.sf.ehcache.util.NamedThreadFactory;

import java.util.concurrent.TimeUnit;

public final class TimerHolder {

    private static final long DEFAULT_TICK_DURATION = 10;

    private TimerHolder() {
    }

    /**
     * Get a singleton instance of {@link Timer}. <br>
     * The tick duration is {@link #DEFAULT_TICK_DURATION}.
     *
     * @return Timer
     */
    public static Timer getTimer() {
        return DefaultInstance.INSTANCE;
    }

    private static class DefaultInstance {
        static final Timer INSTANCE = new HashedWheelTimer(new NamedThreadFactory("DefaultTimer" + DEFAULT_TICK_DURATION),
                DEFAULT_TICK_DURATION, TimeUnit.MILLISECONDS);
    }

}
