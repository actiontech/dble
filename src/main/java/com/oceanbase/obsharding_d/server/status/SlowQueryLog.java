/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.status;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.log.slow.SlowQueryLogProcessor;
import com.oceanbase.obsharding_d.server.trace.TraceResult;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
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
    private volatile int queueOverflowPolicy;
    private static final SlowQueryLog INSTANCE = new SlowQueryLog();
    private volatile SlowQueryLogProcessor processor = new SlowQueryLogProcessor();

    private SlowQueryLog() {
        this.slowTime = SystemConfig.getInstance().getSqlSlowTime();
        this.flushPeriod = SystemConfig.getInstance().getFlushSlowLogPeriod();
        this.flushSize = SystemConfig.getInstance().getFlushSlowLogSize();
        this.queueOverflowPolicy = SystemConfig.getInstance().getSlowQueueOverflowPolicy();
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

    public int getQueueOverflowPolicy() {
        return queueOverflowPolicy;
    }

    public void setQueueOverflowPolicy(int queueOverflowPolicy) {
        this.queueOverflowPolicy = queueOverflowPolicy;
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
        if (this.enableSlowLog && processor != null) {
            processor.cancelFlushLogTask();
            processor.initFlushLogTask();
        }
    }

    public int getFlushSize() {
        return flushSize;
    }

    public void setFlushSize(int flushSize) {
        this.flushSize = flushSize;
    }

    public void putSlowQueryLog(ShardingService service, TraceResult log) {
        processor.putSlowQueryLog(service, log);
    }
}
