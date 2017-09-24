/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

import com.actiontech.dble.backend.datasource.PhysicalDBPool;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Datahost is a group of DB servers which is synchronized with each other
 *
 * @author wuzhih
 */
public class DataHostConfig {
    public static final int NOT_SWITCH_DS = -1;
    public static final int DEFAULT_SWITCH_DS = 1;
    public static final int SYN_STATUS_SWITCH_DS = 2;
    public static final int CLUSTER_STATUS_SWITCH_DS = 3;
    private static final Pattern PATTERN = Pattern.compile("\\s*show\\s+slave\\s+status\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_CLUSTER = Pattern.compile("\\s*show\\s+status\\s+like\\s+'wsrep%'", Pattern.CASE_INSENSITIVE);
    private String name;
    private int maxCon = 128;
    private int minCon = 10;
    private int balance = PhysicalDBPool.BALANCE_NONE;
    private final DBHostConfig[] writeHosts;
    private final Map<Integer, DBHostConfig[]> readHosts;
    private String hearbeatSQL;
    private boolean isShowSlaveSql = false;
    private boolean isShowClusterSql = false;
    private int slaveThreshold = -1;
    private final int switchType;
    private boolean tempReadHostAvailable = false;

    public DataHostConfig(String name,
                          DBHostConfig[] writeHosts, Map<Integer, DBHostConfig[]> readHosts, int switchType, int slaveThreshold, boolean tempReadHostAvailable) {
        super();
        this.name = name;
        this.writeHosts = writeHosts;
        this.readHosts = readHosts;
        this.switchType = switchType;
        this.slaveThreshold = slaveThreshold;
        this.tempReadHostAvailable = tempReadHostAvailable;
    }

    public boolean isTempReadHostAvailable() {
        return this.tempReadHostAvailable;
    }

    public int getSlaveThreshold() {
        return slaveThreshold;
    }

    public int getSwitchType() {
        return switchType;
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
        this.balance = balance;
    }

    public DBHostConfig[] getWriteHosts() {
        return writeHosts;
    }

    public Map<Integer, DBHostConfig[]> getReadHosts() {
        return readHosts;
    }

    public String getHearbeatSQL() {
        return hearbeatSQL;
    }

    public void setHearbeatSQL(String heartbeatSQL) {
        this.hearbeatSQL = heartbeatSQL;
        Matcher matcher = PATTERN.matcher(heartbeatSQL);
        if (matcher.find()) {
            isShowSlaveSql = true;
        }
        Matcher matcher2 = PATTERN_CLUSTER.matcher(heartbeatSQL);
        if (matcher2.find()) {
            isShowClusterSql = true;
        }
    }

    public boolean isShowClusterSql() {
        return this.isShowClusterSql;
    }

}
