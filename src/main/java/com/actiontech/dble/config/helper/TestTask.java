/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.plan.common.ptr.BoolPtr;

import java.io.IOException;

public class TestTask extends Thread {
    private PhysicalDbInstance ds;
    private BoolPtr boolPtr;

    public TestTask(PhysicalDbInstance ds, BoolPtr boolPtr) {
        this.ds = ds;
        this.boolPtr = boolPtr;
    }

    @Override
    public void run() {
        try {
            boolean isConnected = ds.testConnection();
            boolPtr.set(isConnected);
        } catch (IOException e) {
            boolPtr.set(false);
        }
    }
}
