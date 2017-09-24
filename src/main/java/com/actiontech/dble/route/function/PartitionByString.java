/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;
import com.actiontech.dble.route.parser.util.Pair;
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

    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
    }


    public void setHashSlice(String hashSlice) {
        Pair<Integer, Integer> p = sequenceSlicing(hashSlice);
        hashSliceStart = p.getKey();
        hashSliceEnd = p.getValue();
    }


    /**
     * "2" -&gt; (0,2)<br/>
     * "1:2" -&gt; (1,2)<br/>
     * "1:" -&gt; (1,0)<br/>
     * "-1:" -&gt; (-1,0)<br/>
     * ":-1" -&gt; (0,-1)<br/>
     * ":" -&gt; (0,0)<br/>
     */
    public static Pair<Integer, Integer> sequenceSlicing(String slice) {
        int ind = slice.indexOf(':');
        if (ind < 0) {
            int i = Integer.parseInt(slice.trim());
            if (i >= 0) {
                return new Pair<>(0, i);
            } else {
                return new Pair<>(i, 0);
            }
        }
        String left = slice.substring(0, ind).trim();
        String right = slice.substring(1 + ind).trim();
        int start, end;
        if (left.length() <= 0) {
            start = 0;
        } else {
            start = Integer.parseInt(left);
        }
        if (right.length() <= 0) {
            end = 0;
        } else {
            end = Integer.parseInt(right);
        }
        return new Pair<>(start, end);
    }

    @Override
    public void init() {
        partitionUtil = new PartitionUtil(count, length);

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

}
