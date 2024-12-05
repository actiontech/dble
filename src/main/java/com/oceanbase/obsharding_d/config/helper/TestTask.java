/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config.helper;

import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbInstance;
import com.oceanbase.obsharding_d.plan.common.ptr.BoolPtr;

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
