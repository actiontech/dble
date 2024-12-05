/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.log.general;


import com.oceanbase.obsharding_d.log.RotateLogStore;
import com.oceanbase.obsharding_d.server.status.GeneralLog;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.manager.response.GeneralLogCf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class GeneralLogProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralLogProcessor.class);
    private static final GeneralLogProcessor INSTANCE = new GeneralLogProcessor();
    private GeneralLogDisruptor logDelegate;
    private RotateLogStore.LogFileManager logFileManager;
    private volatile boolean enable = false;

    public static GeneralLogProcessor getInstance() {
        return INSTANCE;
    }

    private GeneralLogProcessor() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                close();
            }
        });
    }

    public void start() {
        try {
            enable();
        } catch (IOException e) {
            LOGGER.warn("start general log failed, exception ：{}", e);
        }
    }

    public void enable() throws IOException {
        if (!enable) {
            this.logFileManager = RotateLogStore.getInstance().createFileManager(GeneralLog.getInstance().getGeneralLogFile(), GeneralLog.getInstance().getGeneralLogFileSize());
            this.logDelegate = new GeneralLogDisruptor(logFileManager, GeneralLog.getInstance().getGeneralLogQueueSize());
            this.logDelegate.start();
            enable = true;
            GeneralLogHelper.putGLog(GeneralLogCf.FILE_HEADER);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("enable general log success");
            }
        }
    }

    public void disable() {
        if (enable) {
            if (logDelegate != null) {
                logDelegate.stop();
            }
            if (logFileManager != null) {
                logFileManager.close();
            }
            enable = false;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("disable general log success");
            }
        }
    }

    private void close() {
        if (enable) {
            disable();
            LOGGER.info("close general log file");
        }
    }

    public void putGeneralLog(long connID, String type, String sql) {
        if (enable && GeneralLog.getInstance().isEnableGeneralLog()) {
            GeneralLogEntry entry = new GeneralLogEntry(connID, type, sql);
            if (!logDelegate.tryEnqueue(entry)) {
                handleQueueFull(entry);
            }
        }
    }

    public void putGeneralLog(FrontendService service, byte[] data) {
        if (enable && GeneralLog.getInstance().isEnableGeneralLog()) {
            GeneralLogEntry entry = new GeneralLogEntry(service.getConnection().getId(), data, service.getCharset().getClient());
            if (!logDelegate.tryEnqueue(entry)) {
                handleQueueFull(entry);
            }
        }
    }

    public void putGeneralLog(String content) {
        if (enable && GeneralLog.getInstance().isEnableGeneralLog()) {
            GeneralLogEntry entry = new GeneralLogEntry(content);
            if (!logDelegate.tryEnqueue(entry)) {
                handleQueueFull(entry);
            }
        }
    }

    public boolean isEnable() {
        return enable;
    }

    private void handleQueueFull(final GeneralLogEntry entry) {
        logDelegate.justEnqueue(entry);
    }
}
