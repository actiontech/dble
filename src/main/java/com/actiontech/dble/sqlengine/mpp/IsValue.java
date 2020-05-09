/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp;

/**
 * Created by szf on 2018/8/17.
 */
public class IsValue {


    private final Object value;


    public IsValue(Object value) {
        this.value = value;
    }



    public Object getValue() {
        return value;
    }

}
