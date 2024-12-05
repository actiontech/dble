/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.status;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public final class GeneralLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralLog.class);
    private volatile boolean enableGeneralLog;
    private volatile String generalLogFile;
    private int generalLogFileSize;
    private int generalLogQueueSize;
    // private int generalLogBufferSize;
    private static final GeneralLog INSTANCE = new GeneralLog();

    private GeneralLog() {
        this.enableGeneralLog = SystemConfig.getInstance().getEnableGeneralLog() == 1;
        String logFile;
        if (!(logFile = SystemConfig.getInstance().getGeneralLogFile()).startsWith(String.valueOf(File.separatorChar))) {
            logFile = (SystemConfig.getInstance().getHomePath() + File.separatorChar + logFile).replaceAll(File.separator + "+", File.separator);
        }
        File file = new File(logFile);
        try {
            this.generalLogFile = file.getCanonicalPath();
        } catch (IOException e) {
            LOGGER.warn("Invalid generalLogFile path configuration,exception: {}", e);
            this.generalLogFile = file.getAbsolutePath();
        }
        this.generalLogFileSize = SystemConfig.getInstance().getGeneralLogFileSize();
        this.generalLogQueueSize = SystemConfig.getInstance().getGeneralLogQueueSize();
    }

    public static GeneralLog getInstance() {
        return INSTANCE;
    }

    public boolean isEnableGeneralLog() {
        return enableGeneralLog;
    }

    public void setEnableGeneralLog(boolean enableGeneralLog) {
        this.enableGeneralLog = enableGeneralLog;
    }

    public String getGeneralLogFile() {
        return generalLogFile;
    }

    public void setGeneralLogFile(String generalLogFile) {
        this.generalLogFile = generalLogFile;
    }

    public int getGeneralLogFileSize() {
        return generalLogFileSize;
    }

    public int getGeneralLogQueueSize() {
        return generalLogQueueSize;
    }
}
