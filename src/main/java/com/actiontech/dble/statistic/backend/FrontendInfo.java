package com.actiontech.dble.statistic.backend;

public class FrontendInfo {
    String user;
    String host;
    int port;

    public FrontendInfo(String user, String host, int port) {
        this.user = user;
        this.host = host;
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
