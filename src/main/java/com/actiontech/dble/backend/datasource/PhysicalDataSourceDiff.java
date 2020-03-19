/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.datasource;

/**
 * Created by szf on 2018/7/23.
 */
public class PhysicalDataSourceDiff {
    private String writeHostChangeType = null;


    private PhysicalDataSource selfHost;

    private PhysicalDataSource[] relatedHost;

    PhysicalDataSourceDiff(String writeHostChangeType, PhysicalDataSource selfHost, PhysicalDataSource[] relatedHost) {
        this.writeHostChangeType = writeHostChangeType;
        this.selfHost = selfHost;
        this.relatedHost = relatedHost;
    }


    public String getWriteHostChangeType() {
        return writeHostChangeType;
    }

    public void setWriteHostChangeType(String writeHostChangeType) {
        this.writeHostChangeType = writeHostChangeType;
    }

    public PhysicalDataSource getSelfHost() {
        return selfHost;
    }

    public void setSelfHost(PhysicalDataSource selfHost) {
        this.selfHost = selfHost;
    }

    public PhysicalDataSource[] getRelatedHost() {
        return relatedHost;
    }

    public void setRelatedHost(PhysicalDataSource[] relatedHost) {
        this.relatedHost = relatedHost;
    }


}
