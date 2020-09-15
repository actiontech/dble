/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.model.db;

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

    public DbInstanceConfig(String instanceName, String ip, int port, String url,
                            String user, String password, boolean disabled, boolean primary, boolean usingDecrypt) {
        this.instanceName = instanceName;
        this.ip = ip;
        this.port = port;
        this.url = url;
        this.user = user;
        this.password = password;
        this.disabled = disabled;
        this.primary = primary;
        this.usingDecrypt = usingDecrypt;
    }

    public DbInstanceConfig(String instanceName, String ip, int port, String url, String user, String password, int readWeight, String id, boolean disabled,
                            boolean primary, int maxCon, int minCon, PoolConfig poolConfig, boolean usingDecrypt) {
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

    @Override
    public String toString() {
        return "DbInstanceConfig [hostName=" + instanceName + ", url=" + url + "]";
    }

}
