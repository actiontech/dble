/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;
import com.actiontech.dble.route.util.PartitionUtil;
import com.actiontech.dble.util.SplitUtil;

public final class PartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final long serialVersionUID = -4712399083043025898L;
    protected int[] count;
    protected int[] length;
    protected PartitionUtil partitionUtil;

    private static int[] toIntArray(String string) {
        String[] strs = SplitUtil.split(string, ',', true);
        int[] ints = new int[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            ints[i] = Integer.parseInt(strs[i]);
        }
        return ints;
    }

    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
    }

    @Override
    public void init() {
        partitionUtil = new PartitionUtil(count, length);

    }

    private Integer calculate(long key) {
        return partitionUtil.partition(key);
    }

    @Override
    public Integer calculate(String columnValue) {
        try {
            long key = Long.parseLong(columnValue);
            return calculate(key);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
        }
    }

    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        long begin = Long.parseLong(beginValue);
        long end = Long.parseLong(endValue);
        int partitionLength = partitionUtil.getPartitionLength();
        if (end - begin >= partitionLength || begin > end) { //TODO: optimize begin > end
            return new Integer[0];
        }
        Integer beginNode = 0, endNode = 0;
        beginNode = calculate(begin);
        endNode = calculate(end);

        if (beginNode == null || endNode == null) {
            return new Integer[0];
        }
        if (endNode > beginNode || (endNode.equals(beginNode) && partitionUtil.isSingleNode(begin, end))) {
            int len = endNode - beginNode + 1;
            Integer[] re = new Integer[len];

            for (int i = 0; i < len; i++) {
                re[i] = beginNode + i;
            }
            return re;
        } else {
            int split = partitionUtil.getSegmentLength() - beginNode;
            int len = split + endNode + 1;
            if (endNode.equals(beginNode)) {
                //remove duplicate
                len--;
            }
            Integer[] re = new Integer[len];
            for (int i = 0; i < split; i++) {
                re[i] = beginNode + i;
            }
            for (int i = split; i < len; i++) {
                re[i] = i - split;
            }
            return re;
        }
    }
}
