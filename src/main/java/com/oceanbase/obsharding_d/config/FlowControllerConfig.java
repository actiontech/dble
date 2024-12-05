/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config;

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
