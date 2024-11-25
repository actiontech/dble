/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.config.model.db;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.config.model.db.type.DataBaseType;
import com.oceanbase.obsharding_d.config.util.ConfigException;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DbGroupConfig {
    private static final Pattern HP_PATTERN_SHOW_SLAVE_STATUS = Pattern.compile("\\s*show\\s+slave\\s+status\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern HP_PATTERN_READ_ONLY = Pattern.compile("\\s*select\\s+@@read_only\\s*", Pattern.CASE_INSENSITIVE);
    private String name;
    private int rwSplitMode = PhysicalDbGroup.RW_SPLIT_OFF;
    private DbInstanceConfig writeInstanceConfig;
    private List<DbInstanceConfig> readInstanceConfigList;
    private String heartbeatSQL;
    private boolean isShowSlaveSql = false;
    private boolean isSelectReadOnlySql = false;
    private int delayThreshold;
    private int delayPeriodMillis;
    private String delayDatabase;


    private int heartbeatTimeout = 0;
    private int errorRetryCount = 1;
    private int keepAlive = 60;
    private boolean disableHA;

    public DbGroupConfig(String name,
                         DbInstanceConfig writeInstanceConfig, List<DbInstanceConfig> readInstanceConfigs, int delayThreshold, boolean disableHA) {
        super();
        this.name = name;
        this.writeInstanceConfig = writeInstanceConfig;
        this.readInstanceConfigList = readInstanceConfigs;
        this.delayThreshold = delayThreshold;
        this.disableHA = disableHA;
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
        if (rwSplitMode >= 0 && rwSplitMode <= 3) {
            this.rwSplitMode = rwSplitMode;
        } else {
            throw new ConfigException("dbGroup " + name + " rwSplitMode should be between 0 and 3!");
        }
    }

    public DbInstanceConfig getWriteInstanceConfig() {
        return writeInstanceConfig;
    }

    public List<DbInstanceConfig> getReadInstanceConfigs() {
        return readInstanceConfigList;
    }

    public void setWriteInstanceConfig(DbInstanceConfig writeInstanceConfig) {
        this.writeInstanceConfig = writeInstanceConfig;
    }

    public void addReadInstance(DbInstanceConfig readInstance) {
        this.readInstanceConfigList.add(readInstance);
    }

    public void removeReadInstance(DbInstanceConfig readInstance) {
        this.readInstanceConfigList.remove(readInstance);
    }


    public String getHeartbeatSQL() {
        return heartbeatSQL;
    }

    public void setHeartbeatSQL(String heartbeatSQL) {
        this.heartbeatSQL = heartbeatSQL;
        if (StringUtil.isEmpty(heartbeatSQL)) {
            return;
        }
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
        if (errorRetryCount < 0) {
            throw new ConfigException("dbGroup " + name + " errorRetryCount should be greater than 0!");
        }
        this.errorRetryCount = errorRetryCount;
    }

    public int getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(int keepAlive) {
        if (keepAlive < 0) {
            throw new ConfigException("dbGroup " + name + " keepAlive should be greater than 0!");
        }
        this.keepAlive = keepAlive;
    }

    public boolean isDisableHA() {
        return disableHA;
    }

    public void setDisableHA(boolean disableHA) {
        this.disableHA = disableHA;
    }

    public DataBaseType instanceDatabaseType() {
        return writeInstanceConfig.getDataBaseType();
    }

    public int getDelayPeriodMillis() {
        return delayPeriodMillis;
    }

    public void setDelayPeriodMillis(int delayPeriodMillis) {
        this.delayPeriodMillis = delayPeriodMillis;
    }

    public String getDelayDatabase() {
        return delayDatabase;
    }

    public void setDelayDatabase(String delayDatabase) {
        this.delayDatabase = delayDatabase;
    }

    public boolean equalsBaseInfo(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DbGroupConfig that = (DbGroupConfig) o;

        return rwSplitMode == that.rwSplitMode &&
                isShowSlaveSql == that.isShowSlaveSql &&
                isSelectReadOnlySql == that.isSelectReadOnlySql &&
                delayThreshold == that.delayThreshold &&
                heartbeatTimeout == that.heartbeatTimeout &&
                errorRetryCount == that.errorRetryCount &&
                keepAlive == that.keepAlive &&
                disableHA == that.disableHA &&
                delayPeriodMillis == that.delayPeriodMillis &&
                StringUtil.equals(delayDatabase, that.delayDatabase) &&
                Objects.equals(name, that.name) &&
                Objects.equals(heartbeatSQL, that.heartbeatSQL);
    }

    @Override
    public String toString() {
        return "DbGroupConfig{" +
                "name='" + name + '\'' +
                ", rwSplitMode=" + rwSplitMode +
                ", writeInstanceConfig=" + writeInstanceConfig +
                ", readInstanceConfigs=" + readInstanceConfigList +
                ", heartbeatSQL='" + heartbeatSQL + '\'' +
                ", isShowSlaveSql=" + isShowSlaveSql +
                ", isSelectReadOnlySql=" + isSelectReadOnlySql +
                ", delayThreshold=" + delayThreshold +
                ", delayPeriodMillis=" + delayPeriodMillis +
                ", delayDatabase=" + delayDatabase +
                ", heartbeatTimeout=" + heartbeatTimeout +
                ", errorRetryCount=" + errorRetryCount +
                ", keepAlive=" + keepAlive +
                ", disableHA=" + disableHA +
                '}';
    }
}
