package com.actiontech.dble.sqlengine.mpp;

/**
 * Created by szf on 2018/8/17.
 */
public class IsValue {


    private final Object[] valueList;


    public IsValue(Object[] list) {
        valueList = list;
    }



    public Object[] getValueList() {
        return valueList;
    }

}
