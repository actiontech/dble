/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config;

public class FlowControllerConfig {

    private final boolean enableFlowControl;
    private final int highWaterLevel;
    private final int lowWaterLevel;

    public FlowControllerConfig(boolean enableFlowControl, int highWaterLevel, int lowWaterLevel) {
        this.enableFlowControl = enableFlowControl;
        this.highWaterLevel = highWaterLevel;
        this.lowWaterLevel = lowWaterLevel;
    }

    public boolean isEnableFlowControl() {
        return enableFlowControl;
    }

    public int getHighWaterLevel() {
        return highWaterLevel;
    }

    public int getLowWaterLevel() {
        return lowWaterLevel;
    }
}
