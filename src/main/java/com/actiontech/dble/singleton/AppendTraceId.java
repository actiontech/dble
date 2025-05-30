package com.actiontech.dble.singleton;


import com.actiontech.dble.config.model.SystemConfig;

public final class AppendTraceId {

    private static final AppendTraceId INSTANCE = new AppendTraceId();
    private volatile int value;

    public static AppendTraceId getInstance() {
        return INSTANCE;
    }

    public AppendTraceId() {
        this.value = SystemConfig.getInstance().getAppendTraceId();
    }

    public boolean isEnable() {
        return value == 1;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
