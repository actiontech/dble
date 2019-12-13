/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.helper;

import com.actiontech.dble.backend.datasource.PhysicalDatasource;
import com.actiontech.dble.plan.common.ptr.BoolPtr;

import java.io.IOException;

public class TestTask extends Thread {
    private PhysicalDatasource ds;
    private BoolPtr boolPtr;

    public TestTask(PhysicalDatasource ds, BoolPtr boolPtr) {
        this.ds = ds;
        this.boolPtr = boolPtr;
    }

    @Override
    public void run() {
        try {
            boolean isConnected = ds.testConnection(null);
            boolPtr.set(isConnected);
        } catch (IOException e) {
            boolPtr.set(false);
        }
    }
}
