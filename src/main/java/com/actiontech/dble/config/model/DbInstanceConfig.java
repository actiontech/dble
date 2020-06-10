/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.model;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DbInstanceConfig {

    private long idleTimeout = SystemConfig.DEFAULT_IDLE_TIMEOUT;

    private static final long CONNECTION_TIMEOUT = SECONDS.toMillis(30);
    private static final long CON_HEARTBEAT_TIMEOUT = MILLISECONDS.toMillis(20);

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

    // properties for connection pool
    private volatile int maxTotal = -1;
    private volatile int minIdle = -1;
    private volatile long connectionTimeout = CONNECTION_TIMEOUT;
    private volatile long connectionHeartbeatTimeout = CON_HEARTBEAT_TIMEOUT;
    private int numTestsPerEvictionRun = -1;
    private boolean testOnCreate = false;
    private boolean testOnBorrow = false;
    private boolean testOnReturn = false;
    private boolean testWhileIdle = false;
    private long timeBetweenEvictionRunsMillis = 10000;
    private long evictorShutdownTimeoutMillis = -1L;

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

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(long connectionTimeoutMs) {
        if (connectionTimeoutMs == 0) {
            this.connectionTimeout = Integer.MAX_VALUE;
        } else if (connectionTimeoutMs < 250) {
            throw new IllegalArgumentException("connectionTimeout cannot be less than 250ms");
        } else {
            this.connectionTimeout = connectionTimeoutMs;
        }
    }

    public long getConnectionHeartbeatTimeout() {
        return connectionHeartbeatTimeout;
    }

    public void setConnectionHeartbeatTimeout(long connectionHeartbeatTimeout) {
        this.connectionHeartbeatTimeout = connectionHeartbeatTimeout;
    }

    public long getEvictorShutdownTimeoutMillis() {
        return evictorShutdownTimeoutMillis;
    }

    public void setEvictorShutdownTimeoutMillis(long evictorShutdownTimeoutMillis) {
        this.evictorShutdownTimeoutMillis = evictorShutdownTimeoutMillis;
    }

    public int getNumTestsPerEvictionRun() {
        return numTestsPerEvictionRun;
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    public boolean isTestOnCreate() {
        return testOnCreate;
    }

    public void setTestOnCreate(boolean testOnCreate) {
        this.testOnCreate = testOnCreate;
    }

    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    public boolean isTestWhileIdle() {
        return testWhileIdle;
    }

    public void setTestWhileIdle(boolean testWhileIdle) {
        this.testWhileIdle = testWhileIdle;
    }

    public long getTimeBetweenEvictionRunsMillis() {
        return timeBetweenEvictionRunsMillis;
    }

    public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    @Override
    public String toString() {
        return "DbInstanceConfig [hostName=" + instanceName + ", url=" + url + "]";
    }

}
