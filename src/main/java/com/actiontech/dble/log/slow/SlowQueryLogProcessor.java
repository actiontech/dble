/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.slow;

import com.actiontech.dble.btrace.provider.GeneralProvider;
import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.log.DailyRotateLogStore;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.server.trace.TraceResult;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

public class SlowQueryLogProcessor extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlowQueryLogProcessor.class);
    private BlockingQueue<SlowQueryLogEntry> queue;
    private DailyRotateLogStore store;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;
    private long logSize = 0;
    private long lastLogSize = 0;
    private static final String FILE_HEADER = "/FAKE_PATH/mysqld, Version: FAKE_VERSION. started with:\n" +
            "Tcp port: 3320  Unix socket: FAKE_SOCK\n" +
            "Time                 Id Command    Argument";

    public SlowQueryLogProcessor() {
        this.queue = new LinkedBlockingQueue<>();
        ServerConfig config = DbleServer.getInstance().getConfig();
        SystemConfig systemConfig = config.getSystem();
        this.store = new DailyRotateLogStore(systemConfig.getSlowLogBaseDir(), systemConfig.getSlowLogBaseName(), "log", 64, FILE_HEADER);
        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("SlowLogFlushTimerScheduler-%d").build());
    }

    @Override
    public void run() {
        SlowQueryLogEntry log;
        initFlushLogTask();
        try {
            store.open();
            while (SlowQueryLog.getInstance().isEnableSlowLog()) {
                try {
                    log = queue.poll(100, TimeUnit.MILLISECONDS);

                    if (log == null) {
                        continue;
                    }
                    writeLog(log);
                    logSize++;
                    synchronized (this) {
                        if ((logSize - lastLogSize) % SlowQueryLog.getInstance().getFlushSize() == 0) {
                            flushLog();
                        }
                    }
                } catch (Throwable e) {
                    LOGGER.warn("slow log error:", e);
                }
            }

            // disable slow_query_log, need to place all the remaining elements in the queue
            while (true) {
                try {
                    if ((log = queue.poll()) == null) {
                        break;
                    }
                    writeLog(log);
                    logSize++;
                } catch (Throwable e) {
                    LOGGER.warn("slow log error:", e);
                }
            }
            scheduler.shutdown();
            flushLog();
            store.close();
        } catch (IOException e) {
            LOGGER.info("transaction log error:", e);
            store.close();
        }
    }

    public void initFlushLogTask() {
        scheduledFuture = scheduler.scheduleAtFixedRate(flushLogTask(), SlowQueryLog.getInstance().getFlushPeriod(), SlowQueryLog.getInstance().getFlushPeriod(), TimeUnit.SECONDS);
    }

    public void cancelFlushLogTask() {
        scheduledFuture.cancel(false);
    }

    private synchronized void writeLog(SlowQueryLogEntry log) throws IOException {
        if (log == null)
            return;
        byte[] data;
        try {
            data = log.toString().getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            return;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        store.write(buffer);
    }

    private void flushLog() {
        if (logSize == lastLogSize) {
            return;
        }
        lastLogSize = logSize;
        try {
            store.force(false);
        } catch (IOException e) {
            LOGGER.warn("flush slow log error:", e);
            GeneralProvider.beforeSlowLogClose();
        }
    }

    private Runnable flushLogTask() {
        return new Runnable() {
            @Override
            public void run() {
                synchronized (this) {
                    GeneralProvider.runFlushLogTask();
                    flushLog();
                }
            }
        };
    }

    public void putSlowQueryLog(ServerConnection c, TraceResult log) {
        if (log.isCompleted() && log.getOverAllMilliSecond() > SlowQueryLog.getInstance().getSlowTime()) {
            SlowQueryLogEntry logEntry = new SlowQueryLogEntry(c.getExecuteSql(), log, c.getUser(), c.getHost(), c.getId());
            queue.add(logEntry);
        }
    }
}
