/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.datasource;

/**
 * Created by szf on 2018/7/23.
 */
public class PhysicalDatasourceDiff {
    private String writeHostChangeType = null;


    private PhysicalDatasource selfHost;

    private PhysicalDatasource[] relatedHost;

    PhysicalDatasourceDiff(String writeHostChangeType, PhysicalDatasource selfHost, PhysicalDatasource[] relatedHost) {
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

    public PhysicalDatasource getSelfHost() {
        return selfHost;
    }

    public void setSelfHost(PhysicalDatasource selfHost) {
        this.selfHost = selfHost;
    }

    public PhysicalDatasource[] getRelatedHost() {
        return relatedHost;
    }

    public void setRelatedHost(PhysicalDatasource[] relatedHost) {
        this.relatedHost = relatedHost;
    }


}
