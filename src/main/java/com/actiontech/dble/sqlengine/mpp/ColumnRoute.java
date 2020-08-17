/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.sqlengine.mpp;

import com.alibaba.druid.sql.ast.expr.SQLNullExpr;

import java.util.HashSet;

public class ColumnRoute {
    private Object colValue = null;
    private HashSet<RangeValue> rangeValues = null;
    private HashSet<Object> inValues = null;
    private boolean alwaysFalse = false;


    public ColumnRoute(boolean alwaysFalse) {
        this.alwaysFalse = alwaysFalse;
    }

    public ColumnRoute(Object colValue) {
        this.colValue = colValue;
    }

    public ColumnRoute(RangeValue rangeValue) {
        rangeValues = new HashSet<>();
        this.rangeValues.add(rangeValue);
    }

    public void addRangeValues(HashSet<RangeValue> otherRangeValues) {
        if (this.rangeValues == null) {
            this.rangeValues = new HashSet<>();
        }
        this.rangeValues.addAll(otherRangeValues);
    }

    public ColumnRoute(IsValue value) {
        String stringValue = null;
        if (value.getValue() != null) {
            if (value.getValue() instanceof SQLNullExpr) {
                stringValue = "null";
            }
        }
        this.colValue = stringValue;
    }


    public ColumnRoute(HashSet<Object> inValues) {
        this.inValues = inValues;
    }


    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    public Object getColValue() {
        return colValue;
    }

    public void setColValue(Object colValue) {
        this.colValue = colValue;
    }

    public HashSet<RangeValue> getRangeValues() {
        return rangeValues;
    }

    public HashSet<Object> getInValues() {
        return inValues;
    }

    public void setInValues(HashSet<Object> inValues) {
        this.inValues = inValues;
    }

    public ColumnRoute deepClone() {
        ColumnRoute obj = new ColumnRoute(this.alwaysFalse);
        obj.setColValue(this.colValue);
        if (this.inValues != null) {
            obj.inValues = new HashSet<>();
            obj.inValues.addAll(this.inValues);
        }
        if (this.rangeValues != null) {
            obj.rangeValues = new HashSet<>();
            obj.rangeValues.addAll(this.rangeValues);
        }
        return obj;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (this.getColValue() != null) {
            sb.append("value=").append(this.getColValue());
        } else if (this.getInValues() != null) {
            sb.append("value in ").append(this.getInValues());
        }
        if (this.getRangeValues() != null) {
            if (this.getColValue() != null || this.getInValues() != null) {
                sb.append(", and ");
            }
            int i = 0;
            for (RangeValue rangeValue : this.getRangeValues()) {
                if (i > 0) {
                    sb.append(", and ");
                }
                sb.append("value between ").append(rangeValue.getBeginValue()).append(" and ").append(rangeValue.getEndValue());
                i++;
            }
        }
        return sb.toString();
    }
}
