package com.actiontech.dble.config;

public class FlowControllerConfig {

    private final boolean enableFlowControl;
    private final int start;
    private final int end;

    public FlowControllerConfig(boolean enableFlowControl, int start, int end) {
        this.enableFlowControl = enableFlowControl;
        this.start = start;
        this.end = end;
    }

    public boolean isEnableFlowControl() {
        return enableFlowControl;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }
}
