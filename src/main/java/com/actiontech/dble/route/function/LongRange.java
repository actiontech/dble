/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.function;

import java.io.Serializable;

public class LongRange implements Serializable {
    private final int nodeIndex;
    private final long valueStart;
    private final long valueEnd;
    private int hashCode = 1;

    public LongRange(int nodeIndex, long valueStart, long valueEnd) {
        super();
        this.nodeIndex = nodeIndex;
        this.valueStart = valueStart;
        this.valueEnd = valueEnd;
        if (nodeIndex != 0) {
            hashCode *= nodeIndex;
        }
        if (valueEnd - valueStart != 0) {
            hashCode *= (int) (valueEnd - valueStart);
        }
    }

    public int getNodeIndex() {
        return nodeIndex;
    }

    public long getValueStart() {
        return valueStart;
    }

    public long getValueEnd() {
        return valueEnd;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongRange other = (LongRange) o;
        return other.nodeIndex == nodeIndex &&
                other.valueStart == valueStart &&
                other.valueEnd == valueEnd;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
