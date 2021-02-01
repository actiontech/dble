package com.actiontech.dble.server.status;

import com.actiontech.dble.config.model.SystemConfig;

import java.io.File;

public final class GeneralLog {
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
        this.generalLogFile = file.getAbsolutePath();
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
