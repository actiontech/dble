/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.bean;

/**
 * Created by szf on 2018/1/31.
 */
public class KvBean {

    public static final String DELETE = "delete";
    public static final String UPDATE = "update";
    public static final String ADD = "add";


    private String key;
    private String value;
    private long index;

    public KvBean() {
    }

    public KvBean(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public KvBean(String key, String value, long index) {
        this.key = key;
        this.value = value;
        this.index = index;
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

}
