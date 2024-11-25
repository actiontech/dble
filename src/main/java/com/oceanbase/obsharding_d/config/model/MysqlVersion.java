package com.oceanbase.obsharding_d.config.model;

public class MysqlVersion {
    private int serverMajorVersion = 0;
    private int serverMinorVersion = 0;
    private int serverSubMinorVersion = 0;

    public MysqlVersion() {
    }

    public int getServerMajorVersion() {
        return serverMajorVersion;
    }

    public void setServerMajorVersion(int serverMajorVersion) {
        this.serverMajorVersion = serverMajorVersion;
    }

    public int getServerMinorVersion() {
        return serverMinorVersion;
    }

    public void setServerMinorVersion(int serverMinorVersion) {
        this.serverMinorVersion = serverMinorVersion;
    }

    public int getServerSubMinorVersion() {
        return serverSubMinorVersion;
    }

    public void setServerSubMinorVersion(int serverSubMinorVersion) {
        this.serverSubMinorVersion = serverSubMinorVersion;
    }
}
