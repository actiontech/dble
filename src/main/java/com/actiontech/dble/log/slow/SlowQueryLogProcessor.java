/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.slow;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.log.DailyRotateLogStore;

import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.server.trace.TraceResult;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
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
    private long logSize = 0;
    private long lastLogSize = 0;
    private static final String FILE_HEADER = "/FAKE_PATH/mysqld, Version: FAKE_VERSION. started with:\n" +
            "Tcp port: 3320  Unix socket: FAKE_SOCK\n" +
            "Time                 Id Command    Argument";

    public SlowQueryLogProcessor() {
        this.queue = new LinkedBlockingQueue<>();
        this.store = new DailyRotateLogStore(SystemConfig.getInstance().getSlowLogBaseDir(), SystemConfig.getInstance().getSlowLogBaseName(), "log", 64, FILE_HEADER);
        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("SlowLogFlushTimerScheduler-%d").build());
    }

    @Override
    public void run() {
        SlowQueryLogEntry log;
        scheduler.scheduleAtFixedRate(flushLogTask(), SlowQueryLog.getInstance().getFlushPeriod(), SlowQueryLog.getInstance().getFlushPeriod(), TimeUnit.SECONDS);
        try {
            store.open();
            while (SlowQueryLog.getInstance().isEnableSlowLog()) {
                try {
                    log = queue.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    continue;
                }
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
            }
            // disable slow_query_log, end task
            while ((log = queue.poll()) != null) {
                writeLog(log);
                logSize++;
            }
            scheduler.shutdown();
            flushLog();
            store.close();
        } catch (IOException e) {
            LOGGER.info("transaction log error:", e);
            store.close();
        }
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
            LOGGER.info("transaction log error:", e);
            store.close();
        }
    }

    private Runnable flushLogTask() {
        return new Runnable() {
            @Override
            public void run() {
                synchronized (this) {
                    flushLog();
                }
            }
        };
    }

    public void putSlowQueryLog(ShardingService service, TraceResult log) {
        if (log.isCompleted() && log.getOverAllMilliSecond() > SlowQueryLog.getInstance().getSlowTime()) {
            SlowQueryLogEntry logEntry = new SlowQueryLogEntry(service.getExecuteSql(), log, service.getUser(), service.getConnection().getHost(), service.getConnection().getId());
            queue.add(logEntry);
        }
    }
}
