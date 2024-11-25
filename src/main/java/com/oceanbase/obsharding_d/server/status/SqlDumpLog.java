/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.status;

import com.oceanbase.obsharding_d.config.ProblemReporter;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.util.StartProblemReporter;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.apache.logging.log4j.core.appender.rolling.FileSize;
import org.apache.logging.log4j.core.appender.rolling.action.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlDumpLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDumpLog.class);
    private static final SqlDumpLog INSTANCE = new SqlDumpLog();
    private final ProblemReporter problemReporter = StartProblemReporter.getInstance();
    // switch
    private volatile int enableSqlDumpLog = 0;
    // base param
    private String sqlDumpLogBasePath = "sqldump"; // defaultValue: sqldump
    private String sqlDumpLogFileName = "sqldump.log"; // defaultValue: sqldump.log
    private String sqlDumpLogCompressFilePattern = "${date:yyyy-MM}/sqldump-%d{MM-dd}-%i.log.gz"; // defaultValue: ${date:yyyy-MM}/sqldump-%d{MM-dd}-%i.log.gz
    // policies param (optional)
    private int sqlDumpLogOnStartupRotate = 1; // 1-on, 0-off; defaultValue: 1
    private String sqlDumpLogSizeBasedRotate = "50 MB"; // defaultValue: 50 MB
    private int sqlDumpLogTimeBasedRotate = 1; // defaultValue: 1
    // rollover param (optional)
    private String sqlDumpLogDeleteFileAge = "90d"; // expiration time day; defaultValue: 90d
    private String sqlDumpLogCompressFilePath = "*/sqldump-*.log.gz"; // log.gz path; defaultValue: */sqldump-*.log.gz

    private static final String WARNING_FORMAT = "Property [ %s ] '%s' in bootstrap.cnf is illegal, you may need use the default value %s replaced";

    public void verify() {
        // =======
        String enableSqlDumpLog0 = SystemConfig.getInstance().getEnableSqlDumpLog();
        if (isConfig(enableSqlDumpLog0)) {
            try {
                int num = Integer.parseInt(enableSqlDumpLog0);
                if (num >= 0 && num <= 1)
                    this.enableSqlDumpLog = num;
                else
                    throw new Exception();
            } catch (Exception e) {
                problemReporter.warn(String.format(WARNING_FORMAT, "enableSqlDumpLog", enableSqlDumpLog0, this.enableSqlDumpLog + ""));
            }
        }

        // =======
        String sqlDumpLogBasePath0 = SystemConfig.getInstance().getSqlDumpLogBasePath();
        if (isConfig(sqlDumpLogBasePath0)) {
            this.sqlDumpLogBasePath = sqlDumpLogBasePath0;
        } // else, use default

        String sqlDumpLogFileName0 = SystemConfig.getInstance().getSqlDumpLogFileName();
        if (isConfig(sqlDumpLogFileName0)) {
            this.sqlDumpLogFileName = sqlDumpLogFileName0;
        } // else, use default

        String sqlDumpLogCompressFilePattern0 = SystemConfig.getInstance().getSqlDumpLogCompressFilePattern();
        if (isConfig(sqlDumpLogCompressFilePattern0)) {
            this.sqlDumpLogCompressFilePattern = sqlDumpLogCompressFilePattern0;
        } // else, use default

        // =======
        String sqlDumpLogOnStartupRotate0 = SystemConfig.getInstance().getSqlDumpLogOnStartupRotate();
        if (isConfig(sqlDumpLogOnStartupRotate0)) {
            try {
                sqlDumpLogOnStartupRotate = Integer.parseInt(sqlDumpLogOnStartupRotate0);
            } catch (Exception e) {
                problemReporter.warn(String.format(WARNING_FORMAT, "sqlDumpLogOnStartupRotate", sqlDumpLogOnStartupRotate0, this.sqlDumpLogOnStartupRotate + ""));
            }
        } else {
            if (!StringUtil.isBlank(sqlDumpLogOnStartupRotate0)) { // -1、null
                this.sqlDumpLogOnStartupRotate = -1;
            } // else, use default
        }

        String sqlDumpLogSizeBasedRotate0 = SystemConfig.getInstance().getSqlDumpLogSizeBasedRotate();
        if (!isConfig(sqlDumpLogSizeBasedRotate0)) {
            sqlDumpLogSizeBasedRotate0 = sqlDumpLogSizeBasedRotate;
        } // else, use default
        this.sqlDumpLogSizeBasedRotate = FileSize.parse(sqlDumpLogSizeBasedRotate0, 52428800L) + ""; // default: 50 MB

        String sqlDumpLogTimeBasedRotate0 = SystemConfig.getInstance().getSqlDumpLogTimeBasedRotate();
        if (isConfig(sqlDumpLogTimeBasedRotate0)) {
            try {
                int num = Integer.parseInt(sqlDumpLogTimeBasedRotate0);
                if (num < 1)
                    throw new Exception();
                this.sqlDumpLogTimeBasedRotate = num;
            } catch (Exception e) {
                problemReporter.warn(String.format(WARNING_FORMAT, "sqlDumpLogTimeBasedRotate", sqlDumpLogTimeBasedRotate0, this.sqlDumpLogTimeBasedRotate + ""));
            }
        } else {
            if (!StringUtil.isBlank(sqlDumpLogTimeBasedRotate0)) {  // -1、null
                this.sqlDumpLogTimeBasedRotate = -1;
            } // else, use default
        }

        String sqlDumpLogDeleteFileAge0 = SystemConfig.getInstance().getSqlDumpLogDeleteFileAge();
        if (isConfig(sqlDumpLogDeleteFileAge0)) {
            try {
                Duration.parse(sqlDumpLogDeleteFileAge0);
                this.sqlDumpLogDeleteFileAge = sqlDumpLogDeleteFileAge0;
            } catch (Exception e) {
                problemReporter.warn(String.format(WARNING_FORMAT, "sqlDumpLogDeleteFileAge", sqlDumpLogDeleteFileAge0, this.sqlDumpLogDeleteFileAge));
            }
        } // else, use default

        String sqlDumpLogCompressFilePath0 = SystemConfig.getInstance().getSqlDumpLogCompressFilePath();
        if (isConfig(sqlDumpLogCompressFilePath0)) {
            this.sqlDumpLogCompressFilePath = sqlDumpLogCompressFilePath0;
        }  // else, use default
    }

    private static boolean isConfig(String value) {
        if (value == null)
            return false;
        value = value.trim();
        if (StringUtil.isBlank(value) || value.equals("-1") || value.equalsIgnoreCase("null"))
            return false;
        return true;
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
