/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.route;

import com.oceanbase.obsharding_d.sqlengine.mpp.LoadData;
import com.google.common.base.Objects;

import java.util.Optional;

/**
 * @author ylz
 */
public class LoadDataRouteResultsetNode extends RouteResultsetNode {

    private static final long serialVersionUID = 1L;
    private String name;
    private LoadData loadData;

    public LoadDataRouteResultsetNode(String name, int sqlType, String srcStatement) {
        super(name, sqlType, srcStatement);
    }

    @Override
    public LoadData getLoadData() {
        return loadData;
    }

    @Override
    public void setLoadData(LoadData loadData) {
        this.loadData = loadData;
        Optional.ofNullable(loadData.getFileName()).ifPresent(fileName -> name = fileName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        LoadDataRouteResultsetNode that = (LoadDataRouteResultsetNode) o;
        return Objects.equal(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), name);
    }
}
