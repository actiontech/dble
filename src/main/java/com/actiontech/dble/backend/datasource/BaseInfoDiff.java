/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.datasource;

/**
 * Created by szf on 2018/7/23.
 */
public class BaseInfoDiff {

    String type;


    String orgValue;
    String newVaule;

    BaseInfoDiff(String type, int orgVaule, int newValue) {
        this.type = type;
        this.orgValue = orgVaule + "";
        this.newVaule = newValue + "";
    }


    BaseInfoDiff(String type, String orgVaule, String newValue) {
        this.type = type;
        this.orgValue = orgVaule;
        this.newVaule = newValue;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOrgValue() {
        return orgValue;
    }

    public void setOrgValue(String orgValue) {
        this.orgValue = orgValue;
    }

    public String getNewVaule() {
        return newVaule;
    }

    public void setNewVaule(String newVaule) {
        this.newVaule = newVaule;
    }

}
