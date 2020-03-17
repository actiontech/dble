/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.ptr;

public class BytePtr {
    private byte l;

    public BytePtr(byte l) {
        this.l = l;
    }

    public byte get() {
        return l;
    }

    public void set(byte v) {
        this.l = v;
    }

    public long decre() {
        return l--;
    }

    public long incre() {
        return l++;
    }
}
