/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.ptr;

public class DoublePtr {
    private double db;

    public DoublePtr(double db) {
        this.db = db;
    }

    public double get() {
        return db;
    }

    public void set(double v) {
        this.db = v;
    }
}
