/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine.mpp;

public class RangeValue {
    private Object beginValue;
    private Object endValue;

    public RangeValue(Object beginValue, Object endValue) {
        super();
        this.beginValue = beginValue;
        this.endValue = endValue;
    }

    @Override
    public int hashCode() {
        int hash = beginValue.hashCode();
        hash = hash * 31 + endValue.hashCode();
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

        return true;
    }

    public Object getBeginValue() {
        return beginValue;
    }

    public Object getEndValue() {
        return endValue;
    }

    @Override
    public String toString() {
        return "beginValue:" + beginValue + ",endValue;" + endValue;
    }
}
