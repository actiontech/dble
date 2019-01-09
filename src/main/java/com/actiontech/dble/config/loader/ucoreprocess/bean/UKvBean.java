/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess.bean;

/**
 * Created by szf on 2018/1/31.
 */
public class UKvBean {

    public static final String DELETE = "delete";
    public static final String UPDATE = "update";
    public static final String ADD = "add";


    private String key;
    private String value;
    private String changeType;
    private long index;

    public UKvBean() {
    }

    public UKvBean(String key, String value, long index) {
        this.key = key;
        this.value = value;
        this.index = index;
    }


    public UKvBean(String key, String value, String changeType) {
        this.key = key;
        this.value = value;
        this.changeType = changeType;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public String getChangeType() {
        return changeType;
    }

    public void setChangeType(String changeType) {
        this.changeType = changeType;
    }
}
