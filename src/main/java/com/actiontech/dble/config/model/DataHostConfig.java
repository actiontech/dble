/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.config.util.ConfigException;
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
    private final Map<Integer, DBHostConfig[]> standbyReadHosts;
    private String hearbeatSQL;
    private boolean isShowSlaveSql = false;
    private boolean isShowClusterSql = false;
    private int slaveThreshold = -1;
    private final int switchType;
    private boolean tempReadHostAvailable = false;

    public DataHostConfig(String name,
                          DBHostConfig[] writeHosts, Map<Integer, DBHostConfig[]> readHosts, Map<Integer, DBHostConfig[]> standbyReadHosts, int switchType, int slaveThreshold, int tempReadHostAvailable) {
        super();
        this.name = name;
        this.writeHosts = writeHosts;
        this.readHosts = readHosts;
        this.standbyReadHosts = standbyReadHosts;
        this.switchType = switchType;
        this.slaveThreshold = slaveThreshold;
        this.tempReadHostAvailable = tempReadHostAvailable == 1;
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
        if (balance >= 0 && balance <= 3) {
            this.balance = balance;
        } else {
            throw new ConfigException("dataHost " + name + " balance should be between 0 and 3!");
        }
    }

    public DBHostConfig[] getWriteHosts() {
        return writeHosts;
    }

    public Map<Integer, DBHostConfig[]> getReadHosts() {
        return readHosts;
    }

    public Map<Integer, DBHostConfig[]> getStandbyReadHosts() {
        return standbyReadHosts;
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
        if (switchType == SYN_STATUS_SWITCH_DS && !isShowSlaveSql) {
            throw new ConfigException("if switchType =2 ,the heartbeat must be \"show slave status\"");
        }
        if (switchType == CLUSTER_STATUS_SWITCH_DS && !isShowClusterSql) {
            throw new ConfigException("if switchType =3 ,the heartbeat must be \"show status like 'wsrep%'\"");
        }
    }

    public boolean isShowClusterSql() {
        return this.isShowClusterSql;
    }

}
