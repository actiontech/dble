/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.ptr;

public class GenericPtr<T> {
    private T value;

    public GenericPtr(T initValue) {
        this.value = initValue;
    }

    public T get() {
        return value;
    }

    public void set(T t) {
        this.value = t;
    }

}
