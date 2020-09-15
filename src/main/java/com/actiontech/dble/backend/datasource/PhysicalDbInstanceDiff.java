/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.datasource;

/**
 * Created by szf on 2018/7/23.
 */
public class PhysicalDbInstanceDiff {
    private String writeHostChangeType;

    private PhysicalDbInstance selfInstance;

    private PhysicalDbInstance[] relatedInstance;

    PhysicalDbInstanceDiff(String writeHostChangeType, PhysicalDbInstance selfInstance, PhysicalDbInstance[] relatedInstance) {
        this.writeHostChangeType = writeHostChangeType;
        this.selfInstance = selfInstance;
        this.relatedInstance = relatedInstance;
    }

    public String getWriteHostChangeType() {
        return writeHostChangeType;
    }

    public void setWriteHostChangeType(String writeHostChangeType) {
        this.writeHostChangeType = writeHostChangeType;
    }

    public PhysicalDbInstance getSelfInstance() {
        return selfInstance;
    }

    public void setSelfInstance(PhysicalDbInstance selfInstance) {
        this.selfInstance = selfInstance;
    }

    public PhysicalDbInstance[] getRelatedInstance() {
        return relatedInstance;
    }

    public void setRelatedInstance(PhysicalDbInstance[] relatedInstance) {
        this.relatedInstance = relatedInstance;
    }


}
