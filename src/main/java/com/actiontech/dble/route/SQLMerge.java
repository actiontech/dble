/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route;

import java.io.Serializable;

public class SQLMerge implements Serializable {
    private String[] groupByCols;

    public String[] getGroupByCols() {
        return groupByCols;
    }

    public void setGroupByCols(String[] groupByCols) {
        this.groupByCols = groupByCols;
    }
}
