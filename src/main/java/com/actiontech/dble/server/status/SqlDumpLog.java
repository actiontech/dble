package com.actiontech.dble.server.status;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.util.StartProblemReporter;
import org.apache.logging.log4j.core.appender.rolling.FileSize;
import org.apache.logging.log4j.core.appender.rolling.action.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlDumpLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDumpLog.class);
    private static final SqlDumpLog INSTANCE = new SqlDumpLog();

    private volatile int enableSqlDumpLog = 0;
    private String sqlDumpLogBasePath;
    private String sqlDumpLogFileName;
    private String sqlDumpLogCompressFilePattern;
    private int sqlDumpLogOnStartupRotate;
    private String sqlDumpLogSizeBasedRotate;
    private int sqlDumpLogTimeBasedRotate;
    private String sqlDumpLogDeleteFileAge;
    private String sqlDumpLogCompressFilePath;


    public SqlDumpLog() {
        // switch
        this.enableSqlDumpLog = SystemConfig.getInstance().getEnableSqlDumpLog();
        // base param
        this.sqlDumpLogBasePath = SystemConfig.getInstance().getSqlDumpLogBasePath();
        this.sqlDumpLogFileName = SystemConfig.getInstance().getSqlDumpLogFileName();
        this.sqlDumpLogCompressFilePattern = SystemConfig.getInstance().getSqlDumpLogCompressFilePattern();
        // policies param (optional)
        this.sqlDumpLogOnStartupRotate = SystemConfig.getInstance().getSqlDumpLogOnStartupRotate() == 1 ? 1 : 0;
        this.sqlDumpLogSizeBasedRotate = SystemConfig.getInstance().getSqlDumpLogSizeBasedRotate();
        this.sqlDumpLogTimeBasedRotate = SystemConfig.getInstance().getSqlDumpLogTimeBasedRotate() < 1 ? -1 : SystemConfig.getInstance().getSqlDumpLogTimeBasedRotate();
        // rollover param (optional)
        this.sqlDumpLogDeleteFileAge = SystemConfig.getInstance().getSqlDumpLogDeleteFileAge();
        this.sqlDumpLogCompressFilePath = SystemConfig.getInstance().getSqlDumpLogCompressFilePath();
    }

    public void verify() {
        // '-1' means that it is not configured
        if (!sqlDumpLogSizeBasedRotate.equals("-1")) {
            // default: 50 MB
            sqlDumpLogSizeBasedRotate = FileSize.parse(sqlDumpLogSizeBasedRotate, 52428800L) + "";
        }

        if (!sqlDumpLogDeleteFileAge.equals("-1")) {
            try {
                Duration.parse(sqlDumpLogDeleteFileAge);
            } catch (Exception e) {
                StartProblemReporter.getInstance().warn("parse [sqlDumpLogDeleteFileAge] failed: " + e.getMessage());
            }
            if (sqlDumpLogCompressFilePath.equals("-1")) {
                StartProblemReporter.getInstance().warn("[sqlDumpLogCompressFilePath] can't be null");
            }
        }
    }

    public static SqlDumpLog getInstance() {
        return INSTANCE;
    }

    public void setEnableSqlDumpLog(int enableSqlDumpLog) {
        this.enableSqlDumpLog = enableSqlDumpLog;
    }

    public int getEnableSqlDumpLog() {
        return enableSqlDumpLog;
    }

    public String getSqlDumpLogBasePath() {
        return sqlDumpLogBasePath;
    }

    public String getSqlDumpLogFileName() {
        return sqlDumpLogFileName;
    }

    public String getSqlDumpLogCompressFilePattern() {
        return sqlDumpLogCompressFilePattern;
    }

    public String getSqlDumpLogCompressFilePath() {
        return sqlDumpLogCompressFilePath;
    }

    public int getSqlDumpLogOnStartupRotate() {
        return sqlDumpLogOnStartupRotate;
    }

    public String getSqlDumpLogSizeBasedRotate() {
        return sqlDumpLogSizeBasedRotate;
    }

    public int getSqlDumpLogTimeBasedRotate() {
        return sqlDumpLogTimeBasedRotate;
    }

    public String getSqlDumpLogDeleteFileAge() {
        return sqlDumpLogDeleteFileAge;
    }
}
