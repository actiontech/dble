/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util;

import com.oceanbase.obsharding_d.services.FrontendService;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.function.Supplier;

/**
 * @author dcy
 * Create Date: 2021-09-01
 */
public class DelayServiceControl {
    private static final DelayServiceControl INSTANCE = new DelayServiceControl();
    private final Set<FrontendService> delayServices = Sets.newConcurrentHashSet();

    public static DelayServiceControl getInstance() {
        return INSTANCE;
    }

    public synchronized boolean blockServiceIfNeed(FrontendService frontendService, Supplier<Boolean> needBlock) {
        if (needBlock.get()) {
            delayServices.add(frontendService);
            return true;
        }
        return false;
    }

    public synchronized void wakeUpAllBlockServices() {
        for (FrontendService blockService : delayServices) {
            blockService.notifyTaskThread();
        }
        delayServices.clear();
    }
}
