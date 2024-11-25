/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

/**
 * @author dcy
 * Create Date: 2021-04-02
 */
public final class ViewType {
    String createSql;

    public ViewType() {
    }

    public ViewType(String createSql) {
        this.createSql = createSql;
    }

    public String getCreateSql() {
        return createSql;
    }

    public ViewType setCreateSql(String createSqlTmp) {
        this.createSql = createSqlTmp;
        return this;
    }

    @Override
    public String toString() {
        return "ViewType{" +
                "createSql='" + createSql + '\'' +
                '}';
    }
}
