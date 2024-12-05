/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.ptr;

public class BoolPtr {
    private volatile boolean b;

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
