/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp;

public class RangeValue {
    /*
     * (]
     */
    public static final Integer NE = 0;
    public static final Integer EE = 1;
    public static final Integer EN = 2;
    public static final Integer NN = 3;

    private Object beginValue;
    private Object endValue;
    private Integer rangeType;

    public RangeValue(Object beginValue, Object endValue, Integer rangeType) {
        super();
        this.beginValue = beginValue;
        this.endValue = endValue;
        this.rangeType = rangeType;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash = beginValue.hashCode();
        hash = hash * 31 + endValue.hashCode();
        hash = hash * 31 + rangeType;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RangeValue other = (RangeValue) obj;

        if (beginValue == null) {
            if (other.beginValue != null) {
                return false;
            }
        } else if (!beginValue.equals(other.beginValue)) {
            return false;
        }

        if (endValue == null) {
            if (other.endValue != null) {
                return false;
            }
        } else if (!endValue.equals(other.endValue)) {
            return false;
        }

        if (rangeType == null) {
            if (other.rangeType != null) {
                return false;
            }
        } else if (!rangeType.equals(other.rangeType)) {
            return false;
        }

        return true;
    }

    public Object getBeginValue() {
        return beginValue;
    }

    public void setBeginValue(Object beginValue) {
        this.beginValue = beginValue;
    }

    public Object getEndValue() {
        return endValue;
    }

    public void setEndValue(Object endValue) {
        this.endValue = endValue;
    }

    public Integer getRangeType() {
        return rangeType;
    }

    public void setRangeType(Integer rangeType) {
        this.rangeType = rangeType;
    }
}
