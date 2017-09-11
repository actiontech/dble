/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route;

import com.actiontech.dble.sqlengine.mpp.HavingCols;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class SQLMerge implements Serializable {
    private LinkedHashMap<String, Integer> orderByCols;
    private HavingCols havingCols;
    private Object[] havingColsName;            // Added by winbill, 20160314, for having clause
    private Map<String, Integer> mergeCols;
    private String[] groupByCols;
    private boolean hasAggrColumn;

    public LinkedHashMap<String, Integer> getOrderByCols() {
        return orderByCols;
    }

    public void setOrderByCols(LinkedHashMap<String, Integer> orderByCols) {
        this.orderByCols = orderByCols;
    }

    public Map<String, Integer> getMergeCols() {
        return mergeCols;
    }

    public void setMergeCols(Map<String, Integer> mergeCols) {
        this.mergeCols = mergeCols;
    }

    public String[] getGroupByCols() {
        return groupByCols;
    }

    public void setGroupByCols(String[] groupByCols) {
        this.groupByCols = groupByCols;
    }

    public boolean isHasAggrColumn() {
        return hasAggrColumn;
    }

    public void setHasAggrColumn(boolean hasAggrColumn) {
        this.hasAggrColumn = hasAggrColumn;
    }

    public HavingCols getHavingCols() {
        return havingCols;
    }

    public void setHavingCols(HavingCols havingCols) {
        this.havingCols = havingCols;
    }

    public Object[] getHavingColsName() {
        return havingColsName;
    }

    public void setHavingColsName(Object[] havingColsName) {
        this.havingColsName = havingColsName;
    }
}
