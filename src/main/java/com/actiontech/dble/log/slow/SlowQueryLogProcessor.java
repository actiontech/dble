/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.log.slow;

import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.btrace.provider.GeneralProvider;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.log.DailyRotateLogStore;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.statistic.trace.TraceResult;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
        this.queue = new LinkedBlockingQueue<>(2000);
        this.store = new DailyRotateLogStore(SystemConfig.getInstance().getSlowLogBaseDir(), SystemConfig.getInstance().getSlowLogBaseName(), "log", 64, FILE_HEADER);
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

                    if (writeLog(log)) {
                        logSize++;
                    }

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
                    if (writeLog(log)) {
                        logSize++;
                    }
                } catch (Throwable e) {
                    LOGGER.warn("slow log error:", e);
                }
            }
        } catch (IOException e) {
            LOGGER.warn("transaction log error:", e);
            store.close();
        } finally {
            scheduler.shutdown();
            flushLog();
            store.close();
        }
    }

    public void initFlushLogTask() {
        scheduledFuture = scheduler.scheduleAtFixedRate(flushLogTask(), SlowQueryLog.getInstance().getFlushPeriod(), SlowQueryLog.getInstance().getFlushPeriod(), TimeUnit.SECONDS);
    }

    public void cancelFlushLogTask() {
        scheduledFuture.cancel(false);
    }


    private synchronized boolean writeLog(SlowQueryLogEntry log) throws IOException {
        if (log == null) {
            return false;
        }
        byte[] data;
        try {
            data = log.toString().getBytes("utf-8");
        } catch (Exception e) {
            LOGGER.warn("generate write log error ", e);
            return false;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        store.write(buffer);
        return true;
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

    public void putSlowQueryLog(ShardingService service, TraceResult log) {
        SlowQueryLogEntry logEntry = new SlowQueryLogEntry(service.getExecuteSql(), log, service.getUser(), service.getConnection().getHost(), service.getConnection().getId());

        try {
            boolean enQueue = queue.offer(logEntry);
            if (!enQueue && SlowQueryLog.getInstance().getQueueOverflowPolicy() == 1) {
                //abort
                String errorMsg = "since there are too many slow query logs to be written, some slow query logs will be discarded so as not to affect business requirements. Discard log entry: {" + logEntry.toString() + "}";
                LOGGER.warn(errorMsg);
                // alert
                AlertUtil.alertSelf(AlarmCode.SLOW_QUERY_QUEUE_POLICY_ABORT, Alert.AlertLevel.WARN, errorMsg, null);
            } else if (!enQueue && SlowQueryLog.getInstance().getQueueOverflowPolicy() == 2) {
                //wait 3s
                long start = System.nanoTime();
                boolean offerFlag = queue.offer(logEntry, 3, TimeUnit.SECONDS);
                String costTime = String.valueOf((System.nanoTime() - start) / 1000000);
                if (offerFlag) {
                    String errorMsg = "since there are too many slow query logs to be written, the write channel is blocked, and the returned slow SQL execution time will include the write channel blocking time:" + costTime + "(ms).";
                    LOGGER.warn(errorMsg);
                    // alert
                    AlertUtil.alertSelf(AlarmCode.SLOW_QUERY_QUEUE_POLICY_WAIT, Alert.AlertLevel.WARN, errorMsg, null);
                } else {
                    LOGGER.warn("slow log queue has so many item and waiting time:3s exceeded. Discard log entry: {} ", logEntry.toString());
                }
            }
        } catch (InterruptedException e) {
            LOGGER.info(" ", e);
        }
    }
}
