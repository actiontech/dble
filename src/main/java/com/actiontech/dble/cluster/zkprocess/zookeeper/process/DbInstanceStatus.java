package com.actiontech.dble.cluster.zkprocess.zookeeper.process;

/**
 * Created by szf on 2019/10/29.
 */
public class DbInstanceStatus {
    private String name;
    private boolean disable;
    private boolean primary;

    public DbInstanceStatus(String name, boolean disable, boolean primary) {
        this.name = name;
        this.disable = disable;
        this.primary = primary;
    }

    public String getName() {
        return name;
    }

    public boolean isDisable() {
        return disable;
    }

    public boolean isPrimary() {
        return primary;
    }
}
