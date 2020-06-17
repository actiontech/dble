/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.model.db;

import com.actiontech.dble.config.model.SystemConfig;

public class DbInstanceConfig {

    private long idleTimeout = SystemConfig.DEFAULT_IDLE_TIMEOUT;
    private final String instanceName;
    private final String ip;
    private final int port;
    private final String url;
    private final String user;
    private final String password;
    private int maxCon;
    private int minCon;
    private int readWeight;
    private String id;

    private boolean disabled = false;

    private boolean primary = false;

    public DbInstanceConfig(String instanceName, String ip, int port, String url,
                            String user, String password, boolean disabled, boolean primary) {
        this.instanceName = instanceName;
        this.ip = ip;
        this.port = port;
        this.url = url;
        this.user = user;
        this.password = password;
        this.disabled = disabled;
        this.primary = primary;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
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

    @Override
    public String toString() {
        return "DbInstanceConfig [hostName=" + instanceName + ", url=" + url + "]";
    }

}
