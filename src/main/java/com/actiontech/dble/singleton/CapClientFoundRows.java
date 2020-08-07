package com.actiontech.dble.singleton;


import com.actiontech.dble.config.model.SystemConfig;

public final class CapClientFoundRows {

    private static final CapClientFoundRows INSTANCE = new CapClientFoundRows();
    private volatile boolean enableCapClientFoundRows;

    public static CapClientFoundRows getInstance() {
        return INSTANCE;
    }

    public CapClientFoundRows() {
        this.enableCapClientFoundRows = SystemConfig.getInstance().isCapClientFoundRows();
    }

    public boolean isEnableCapClientFoundRows() {
        return enableCapClientFoundRows;
    }

    public void setEnableCapClientFoundRows(boolean enableCapClientFoundRows) {
        this.enableCapClientFoundRows = enableCapClientFoundRows;
    }
}
