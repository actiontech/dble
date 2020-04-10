package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.FlowCotrollerConfig;

/**
 * Created by szf on 2020/4/9.
 */
public final class WriteQueueFlowController {
    private static final WriteQueueFlowController INSTANCE = new WriteQueueFlowController();
    private volatile FlowCotrollerConfig config = null;

    private WriteQueueFlowController() {
    }

    public static void init() throws Exception {
        INSTANCE.config = new FlowCotrollerConfig(
                DbleServer.getInstance().getConfig().getSystem().isEnableFlowControl(),
                DbleServer.getInstance().getConfig().getSystem().getFlowControlStartThreshold(),
                DbleServer.getInstance().getConfig().getSystem().getFlowControlStopThreshold());
        if (INSTANCE.config.getEnd() < 0 || INSTANCE.config.getStart() <= 0) {
            throw new Exception("The flowControlStartThreshold & flowControlStopThreshold must be positive integer");
        } else if (INSTANCE.config.getEnd() >= INSTANCE.config.getStart()) {
            throw new Exception("The flowControlStartThreshold must bigger than flowControlStopThreshold");
        }
    }

    public static FlowCotrollerConfig getFlowCotrollerConfig() {
        return INSTANCE.config;
    }

    public static void configChange(FlowCotrollerConfig newConfig) {
        INSTANCE.config = newConfig;
    }

    public static boolean isEnableFlowControl() {
        return INSTANCE.config.isEnableFlowControl();
    }

    public static int getFlowStart() {
        return INSTANCE.config.getStart();
    }
}


