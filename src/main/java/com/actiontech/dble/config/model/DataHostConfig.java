/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

import com.actiontech.dble.backend.datasource.AbstractPhysicalDBPool;
import com.actiontech.dble.config.util.ConfigException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Datahost is a group of DB servers which is synchronized with each other
 *
 * @author wuzhih
 */
public class DataHostConfig {
    private static final Pattern HP_PATTERN_SHOW_SLAVE_STATUS = Pattern.compile("\\s*show\\s+slave\\s+status\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern HP_PATTERN_READ_ONLY = Pattern.compile("\\s*select\\s+@@read_only\\s*", Pattern.CASE_INSENSITIVE);
    private String name;
    private int maxCon = 128;
    private int minCon = 10;
    private int balance = AbstractPhysicalDBPool.BALANCE_NONE;
    private final DBHostConfig writeHost;
    private final DBHostConfig[] readHosts;
    private String hearbeatSQL;
    private boolean isShowSlaveSql = false;
    private boolean isSelectReadOnlySql = false;
    private int slaveThreshold = -1;
    private boolean tempReadHostAvailable = false;

    private int heartbeatTimeout = 0;
    private int errorRetryCount = 0;

    public DataHostConfig(String name,
                          DBHostConfig writeHost, DBHostConfig[] readHosts, int slaveThreshold, int tempReadHostAvailable) {
        super();
        this.name = name;
        this.writeHost = writeHost;
        this.readHosts = readHosts;
        this.slaveThreshold = slaveThreshold;
        this.tempReadHostAvailable = tempReadHostAvailable == 1;
    }

    public boolean isTempReadHostAvailable() {
        return this.tempReadHostAvailable;
    }

    public int getSlaveThreshold() {
        return slaveThreshold;
    }

    public String getName() {
        return name;
    }

    public boolean isShowSlaveSql() {
        return isShowSlaveSql;
    }

    public int getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(int maxCon) {
        this.maxCon = maxCon;
    }

    public int getMinCon() {
        return minCon;
    }

    public void setMinCon(int minCon) {
        this.minCon = minCon;
    }

    public int getBalance() {
        return balance;
    }

    public void setBalance(int balance) {
        if (balance >= 0 && balance <= 2) {
            this.balance = balance;
        } else {
            throw new ConfigException("dataHost " + name + " balance should be between 0 and 2!");
        }
    }

    public DBHostConfig getWriteHost() {
        return writeHost;
    }

    public DBHostConfig[] getReadHosts() {
        return readHosts;
    }


    public String getHearbeatSQL() {
        return hearbeatSQL;
    }

    public void setHearbeatSQL(String heartbeatSQL) {
        this.hearbeatSQL = heartbeatSQL;
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
