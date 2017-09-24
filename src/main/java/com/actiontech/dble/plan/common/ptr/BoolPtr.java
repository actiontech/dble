/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.ptr;

public class BoolPtr {
    private boolean b;

    public BoolPtr(boolean b) {
        this.b = b;
    }

    public boolean get() {
        return b;
    }

    public void set(boolean bool) {
        this.b = bool;
    }
}
