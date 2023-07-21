/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.model.db;

import com.actiontech.dble.config.model.db.type.DataBaseType;
import com.actiontech.dble.util.StringUtil;

import java.util.Objects;

public class DbInstanceConfig {

    private final String instanceName;
    private final String ip;
    private final int port;
    private final String url;
    private final String user;
    private final String password;
    private int readWeight;
    private String id;
    private boolean disabled;
    private boolean primary;
    private volatile int maxCon = -1;
    private volatile int minCon = -1;
    private volatile PoolConfig poolConfig;
    private final boolean usingDecrypt;
    private DataBaseType dataBaseType;
    private String dbDistrict;
    private String dbDataCenter;

    public DbInstanceConfig(String instanceName, String ip, int port, String url,
                            String user, String password, boolean disabled, boolean primary, boolean usingDecrypt, DataBaseType dataBaseType) {
        this.instanceName = instanceName;
        this.ip = ip;
        this.port = port;
        this.url = url;
        this.user = user;
        this.password = password;
        this.disabled = disabled;
        this.primary = primary;
        this.usingDecrypt = usingDecrypt;
        this.dataBaseType = dataBaseType;
    }

    public DbInstanceConfig(String instanceName, String ip, int port, String url, String user, String password, int readWeight, String id, boolean disabled,
                            boolean primary, int maxCon, int minCon, PoolConfig poolConfig, boolean usingDecrypt, DataBaseType dataBaseType) {
        this.instanceName = instanceName;
        this.ip = ip;
        this.port = port;
        this.url = url;
        this.user = user;
        this.password = password;
        this.readWeight = readWeight;
        this.id = id;
        this.disabled = disabled;
        this.primary = primary;
        this.maxCon = maxCon;
        this.minCon = minCon;
        this.poolConfig = poolConfig;
        this.usingDecrypt = usingDecrypt;
        this.dataBaseType = dataBaseType;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public int getReadWeight() {
        return readWeight;
    }

    public void setReadWeight(int readWeight) {
        this.readWeight = readWeight;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isPrimary() {
        return primary;
    }

    public void setPrimary(boolean primary) {
        this.primary = primary;
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

    public PoolConfig getPoolConfig() {
        return poolConfig;
    }

    public void setPoolConfig(PoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public boolean isUsingDecrypt() {
        return usingDecrypt;
    }

    public DataBaseType getDataBaseType() {
        return dataBaseType;
    }

    public String getDbDistrict() {
        return dbDistrict;
    }

    public void setDbDistrict(String dbDistrict) {
        this.dbDistrict = dbDistrict;
    }

    public String getDbDataCenter() {
        return dbDataCenter;
    }

    public void setDbDataCenter(String dbDataCenter) {
        this.dbDataCenter = dbDataCenter;
    }

    @Override
    public String toString() {
        return "DbInstanceConfig [hostName=" + instanceName + ", url=" + url + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DbInstanceConfig that = (DbInstanceConfig) o;
        return port == that.port &&
                readWeight == that.readWeight &&
                disabled == that.disabled &&
                primary == that.primary &&
                maxCon == that.maxCon &&
                minCon == that.minCon &&
                usingDecrypt == that.usingDecrypt &&
                dataBaseType.equals(((DbInstanceConfig) o).dataBaseType) &&
                Objects.equals(instanceName, that.instanceName) &&
                Objects.equals(ip, that.ip) &&
                Objects.equals(url, that.url) &&
                Objects.equals(user, that.user) &&
                Objects.equals(password, that.password) &&
                Objects.equals(id, that.id) &&
                Objects.equals(poolConfig, that.poolConfig) &&
                StringUtil.equalsIgnoreCase(dbDistrict, that.getDbDistrict()) &&
                StringUtil.equalsIgnoreCase(dbDataCenter, that.getDbDataCenter());
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceName, ip, port, url, user, password, readWeight,
                id, disabled, primary, maxCon, minCon, poolConfig, usingDecrypt,
                dataBaseType, dbDistrict, dbDataCenter);
    }
}
