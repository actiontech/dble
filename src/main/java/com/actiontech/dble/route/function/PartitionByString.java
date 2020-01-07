/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.parser.util.PairUtil;
import com.actiontech.dble.route.util.PartitionUtil;
import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;


/**
 * @author <a href="mailto:daasadmin@hp.com">yangwenx</a>
 */
public final class PartitionByString extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    private static final long serialVersionUID = 3777423001153345948L;
    private int hashSliceStart = 0;
    /**
     * 0 means str.length(), -1 means str.length()-1
     */
    private int hashSliceEnd = 8;
    protected int[] count;
    protected int[] length;
    protected PartitionUtil partitionUtil;
    private int hashCode = 1;

    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
        propertiesMap.put("partitionCount", partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
        propertiesMap.put("partitionLength", partitionLength);
    }


    public void setHashSlice(String hashSlice) {
        Pair<Integer, Integer> p = PairUtil.sequenceSlicing(hashSlice);
        hashSliceStart = p.getKey();
        hashSliceEnd = p.getValue();
        propertiesMap.put("hashSlice", hashSlice);
    }

    @Override
    public void init() {
        partitionUtil = new PartitionUtil(count, length);
        initHashCode();

    }

    @Override
    public void selfCheck() {
    }

    private static int[] toIntArray(String string) {
        String[] strs = SplitUtil.split(string, ',', true);
        int[] ints = new int[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            ints[i] = Integer.parseInt(strs[i]);
        }
        return ints;
    }

    @Override
    public Integer calculate(String key) {
        if (key == null || key.equalsIgnoreCase("NULL")) {
            return 0;
        }
        int start = hashSliceStart >= 0 ? hashSliceStart : key.length() + hashSliceStart;
        int end = hashSliceEnd > 0 ? hashSliceEnd : key.length() + hashSliceEnd;
        long hash = StringUtil.hash(key, start, end);
        return partitionUtil.partition(hash);
    }

    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        //all node
        return new Integer[0];
    }

    @Override
    public int getPartitionNum() {
        int nPartition = 0;
        for (int aCount : count) {
            nPartition += aCount;
        }
        return nPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionByString other = (PartitionByString) o;
        if (other.count.length != this.count.length || other.length.length != this.length.length) {
            return false;
        }
        for (int i = 0; i < count.length; i++) {
            if (this.count[i] != other.count[i]) {
                return false;
            }
        }
        for (int i = 0; i < length.length; i++) {
            if (this.length[i] != other.length[i]) {
                return false;
            }
        }
        return hashSliceStart == other.hashSliceStart && hashSliceEnd == other.hashSliceEnd;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private void initHashCode() {
        for (int aCount : count) {
            hashCode *= aCount;
        }
        for (int aLength : length) {
            hashCode *= aLength;
        }
        if (hashSliceEnd - hashSliceStart != 0) {
            hashCode *= hashSliceEnd - hashSliceStart;
        }
    }
}
