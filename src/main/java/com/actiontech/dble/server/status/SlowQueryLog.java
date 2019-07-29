/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.status;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.log.slow.SlowQueryLogProcessor;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.trace.TraceResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class SlowQueryLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlowQueryLog.class);
    private volatile boolean enableSlowLog = false;
    private volatile int slowTime; //ms
    private volatile int flushPeriod;
    private volatile int flushSize;
    private static final SlowQueryLog INSTANCE = new SlowQueryLog();
    private volatile SlowQueryLogProcessor processor = new SlowQueryLogProcessor();

    private SlowQueryLog() {
        this.slowTime = DbleServer.getInstance().getConfig().getSystem().getSqlSlowTime();
        this.flushPeriod = DbleServer.getInstance().getConfig().getSystem().getFlushSlowLogPeriod();
        this.flushSize = DbleServer.getInstance().getConfig().getSystem().getFlushSlowLogSize();
    }

    public static SlowQueryLog getInstance() {
        return INSTANCE;
    }

    public boolean isEnableSlowLog() {
        return enableSlowLog;
    }

    public void setEnableSlowLog(boolean enableSlow) {
        if (enableSlow) {
            if (enableSlowLog) {
                return;
            }
            while (processor.isAlive()) {
                LOGGER.warn("last slow query log is writing async, so wait 100ms");
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
            processor = new SlowQueryLogProcessor();
            processor.setName("SlowQueryLogProcessor");
            this.enableSlowLog = true;
            processor.start();
        } else {
            this.enableSlowLog = false;
        }
    }

    public int getSlowTime() {
        return slowTime;
    }

    public void setSlowTime(int slowTime) {
        this.slowTime = slowTime;
    }

    public int getFlushPeriod() {
        return flushPeriod;
    }

    public void setFlushPeriod(int flushPeriod) {
        this.flushPeriod = flushPeriod;
    }

    public int getFlushSize() {
        return flushSize;
    }

    public void setFlushSize(int flushSize) {
        this.flushSize = flushSize;
    }

    public void putSlowQueryLog(ServerConnection c, TraceResult log) {
        processor.putSlowQueryLog(c, log);
    }
}
