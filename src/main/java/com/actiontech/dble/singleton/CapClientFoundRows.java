/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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
