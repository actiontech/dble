/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.ptr;

public class StringPtr {
    private String s;

    public StringPtr(String s) {
        this.s = s;
    }

    public String get() {
        return s;
    }

    public void set(String str) {
        this.s = str;
    }
}
