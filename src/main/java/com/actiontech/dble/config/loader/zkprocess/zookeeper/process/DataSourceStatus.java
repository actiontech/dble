package com.actiontech.dble.config.loader.zkprocess.zookeeper.process;

/**
 * Created by szf on 2019/10/29.
 */
public class DataSourceStatus {
    String name;
    boolean disable;
    boolean writeHost;

    public DataSourceStatus(String name, boolean disable, boolean isWriteHost) {
        this.name = name;
        this.disable = disable;
        this.writeHost = isWriteHost;
    }

    public String getName() {
        return name;
    }

    public boolean isDisable() {
        return disable;
    }

    public boolean isWriteHost() {
        return writeHost;
    }
}
