/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.ptr;

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
