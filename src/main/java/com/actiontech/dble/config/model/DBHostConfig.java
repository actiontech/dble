/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.model;

public class DBHostConfig {

    private long idleTimeout = SystemConfig.DEFAULT_IDLE_TIMEOUT;
    private final String hostName;
    private final String ip;
    private final int port;
    private final String url;
    private final String user;
    private final String password;
    private int maxCon;
    private int minCon;
    private int weight;

    public DBHostConfig(String hostName, String ip, int port, String url,
                        String user, String password) {
        super();
        this.hostName = hostName;
        this.ip = ip;
        this.port = port;
        this.url = url;
        this.user = user;
        this.password = password;
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

    public String getHostName() {
        return hostName;
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

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        return "DBHostConfig [hostName=" + hostName + ", url=" + url + "]";
    }

}
