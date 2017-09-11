/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.ptr;

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
