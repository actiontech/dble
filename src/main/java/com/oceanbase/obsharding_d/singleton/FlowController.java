/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.config.FlowControllerConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class FlowController {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlowController.class);
    private static final FlowController INSTANCE = new FlowController();
    private volatile FlowControllerConfig config = null;

    private FlowController() {
    }

    public static void init() throws Exception {
        boolean enableFlowControl = SystemConfig.getInstance().isEnableFlowControl();
        if (enableFlowControl && SystemConfig.getInstance().getUsingAIO() == 1) {
            enableFlowControl = false;
            LOGGER.warn("flow control is not support AIO, please check property [usingAIO] of your bootstrap.cnf");
        }
        INSTANCE.config = new FlowControllerConfig(enableFlowControl,
                SystemConfig.getInstance().getFlowControlHighLevel(),
                SystemConfig.getInstance().getFlowControlLowLevel());

        if (INSTANCE.config.getLowWaterLevel() <= 0 || INSTANCE.config.getHighWaterLevel() <= 0) {
            throw new Exception("The flowControlHighLevel & flowControlLowLevel must be positive integer");
        } else if (INSTANCE.config.getLowWaterLevel() >= INSTANCE.config.getHighWaterLevel()) {
            throw new Exception("The flowControlHighLevel must bigger than flowControlLowLevel");
        }
    }

    // load data
    public static void checkFlowControl(final MySQLResponseService backendService) {
        while (backendService.getConnection().isBackendWriteFlowControlled()) {
            LOGGER.info("wait 1s before read from file because of flow control");
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
    }

    public static FlowControllerConfig getFlowControllerConfig() {
        return INSTANCE.config;
    }

    public static void configChange(FlowControllerConfig newConfig) {
        INSTANCE.config = newConfig;
    }

    public static boolean isEnableFlowControl() {
        return INSTANCE.config.isEnableFlowControl();
    }

    public static int getFlowHighLevel() {
        return INSTANCE.config.getHighWaterLevel();
    }

    public static int getFlowLowLevel() {
        return INSTANCE.config.getLowWaterLevel();
    }
}


