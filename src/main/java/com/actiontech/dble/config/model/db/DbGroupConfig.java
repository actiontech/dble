/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.model.db;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.config.util.ConfigException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DbGroupConfig {
    private static final Pattern HP_PATTERN_SHOW_SLAVE_STATUS = Pattern.compile("\\s*show\\s+slave\\s+status\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern HP_PATTERN_READ_ONLY = Pattern.compile("\\s*select\\s+@@read_only\\s*", Pattern.CASE_INSENSITIVE);
    private String name;
    private int rwSplitMode = PhysicalDbGroup.RW_SPLIT_OFF;
    private final DbInstanceConfig writeInstanceConfig;
    private final DbInstanceConfig[] readInstanceConfigs;
    private String heartbeatSQL;
    private boolean isShowSlaveSql = false;
    private boolean isSelectReadOnlySql = false;
    private int delayThreshold = -1;

    private int heartbeatTimeout = 0;
    private int errorRetryCount = 0;

    public DbGroupConfig(String name,
                         DbInstanceConfig writeInstanceConfig, DbInstanceConfig[] readInstanceConfigs, int delayThreshold) {
        super();
        this.name = name;
        this.writeInstanceConfig = writeInstanceConfig;
        this.readInstanceConfigs = readInstanceConfigs;
        this.delayThreshold = delayThreshold;
    }

    public int getDelayThreshold() {
        return delayThreshold;
    }

    public String getName() {
        return name;
    }

    public boolean isShowSlaveSql() {
        return isShowSlaveSql;
    }

    public int getRwSplitMode() {
        return rwSplitMode;
    }

    public void setRwSplitMode(int rwSplitMode) {
        if (rwSplitMode >= 0 && rwSplitMode <= 2) {
            this.rwSplitMode = rwSplitMode;
        } else {
            throw new ConfigException("dbGroup " + name + " rwSplitMode should be between 0 and 2!");
        }
    }

    public DbInstanceConfig getWriteInstanceConfig() {
        return writeInstanceConfig;
    }

    public DbInstanceConfig[] getReadInstanceConfigs() {
        return readInstanceConfigs;
    }

    public String getHeartbeatSQL() {
        return heartbeatSQL;
    }

    public void setHeartbeatSQL(String heartbeatSQL) {
        this.heartbeatSQL = heartbeatSQL;
        Matcher matcher = HP_PATTERN_SHOW_SLAVE_STATUS.matcher(heartbeatSQL);
        if (matcher.find()) {
            isShowSlaveSql = true;
        }
        Matcher matcher3 = HP_PATTERN_READ_ONLY.matcher(heartbeatSQL);
        if (matcher3.find()) {
            isSelectReadOnlySql = true;
        }
    }

    public boolean isSelectReadOnlySql() {
        return isSelectReadOnlySql;
    }

    public int getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public void setHeartbeatTimeout(int heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public int getErrorRetryCount() {
        return errorRetryCount;
    }

    public void setErrorRetryCount(int errorRetryCount) {
        this.errorRetryCount = errorRetryCount;
    }
}
