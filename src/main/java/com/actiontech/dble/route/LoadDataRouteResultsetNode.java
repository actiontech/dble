/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.route;

import com.actiontech.dble.sqlengine.mpp.LoadData;
import com.google.common.base.Objects;

/**
 * @author ylz
 */
public class LoadDataRouteResultsetNode extends RouteResultsetNode {

    private static final long serialVersionUID = 1L;
    private String name;
    private LoadData loadData;
    private volatile byte loadDataRrnStatus;

    public LoadDataRouteResultsetNode(String name, int sqlType, String srcStatement) {
        super(name, sqlType, srcStatement);
        loadDataRrnStatus = 0;
    }

    public byte getLoadDataRrnStatus() {
        return loadDataRrnStatus;
    }

    public void setLoadDataRrnStatus(byte loadDataRrnStatus) {
        this.loadDataRrnStatus = loadDataRrnStatus;
    }

    @Override
    public LoadData getLoadData() {
        return loadData;
    }

    @Override
    public void setLoadData(LoadData loadData) {
        this.loadData = loadData;
        name = loadData.getFileName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (name == null) return false;
        if (!super.equals(o)) return false;
        LoadDataRouteResultsetNode that = (LoadDataRouteResultsetNode) o;
        return loadDataRrnStatus == that.loadDataRrnStatus && Objects.equal(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), name);
    }
}
