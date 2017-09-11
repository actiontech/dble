/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.util;

import com.actiontech.dble.util.StringUtil;

import java.io.Serializable;

/**
 * PartitionUtil
 *
 * @author mycat
 */
public final class PartitionUtil implements Serializable {

    // MAX_PARTITION_LENGTH: if the number is 2^n,  then optimizer by x % 2^n == x & (2^n - 1).
    private static final int MAX_PARTITION_LENGTH = 2880;
    private int partitionLength;

    // cached the value of  2^n - 1 because of x % 2^n == x & (2^n - 1).
    private long addValue;

    private int[] segment;

    private boolean canProfile = false;

    private int segmentLength = 0;

    /**
     * <pre>
     * @param count the size of partitions
     * @param length the consequent value of every partition
     * Notice:count.length must equals length.length.
     * and :MAX_PARTITION_LENGTH >=sum((count[i]*length[i]))
     * </pre>
     */
    public PartitionUtil(int[] count, int[] length) {
        if (count == null || length == null || (count.length != length.length)) {
            throw new RuntimeException("error,check your scope & scopeLength definition.");
        }
        for (int aCount : count) {
            if (aCount <= 0) {
                throw new RuntimeException("error,check your scope at least 1.");
            }
            segmentLength += aCount;
        }
        int[] ai = new int[segmentLength + 1];

        int index = 0;
        for (int i = 0; i < count.length; i++) {
            for (int j = 0; j < count[i]; j++) {
                ai[++index] = ai[index - 1] + length[i];
            }
        }
        partitionLength = ai[ai.length - 1];
        addValue = partitionLength - 1;
        segment = new int[partitionLength];
        if (partitionLength > MAX_PARTITION_LENGTH) {
            throw new RuntimeException("error,check your partitionScope definition.MAX(sum(count*length[i]) must be less then 1024 ");
        }
        if ((partitionLength & addValue) == 0) {
            canProfile = true;
        }

        for (int i = 1; i < ai.length; i++) {
            for (int j = ai[i - 1]; j < ai[i]; j++) {
                segment[j] = (i - 1);
            }
        }
    }

    public boolean isSingleNode(long begin, long end) {
        if (begin == end)
            return true;
        int mod = (int) (begin % partitionLength);
        if (mod < 0) {
            mod += partitionLength;
        }
        return begin - mod + addValue >= end;
    }

    public int partition(long hash) {
        if (canProfile) {
            return segment[(int) (hash & addValue)];
        } else {
            int mod = (int) (hash % partitionLength);
            if (mod < 0) {
                mod += partitionLength;
            }
            return segment[mod];
        }
    }

    public int partition(String key, int start, int end) {
        return partition(StringUtil.hash(key, start, end));
    }


    public int getPartitionLength() {
        return partitionLength;
    }

    public int getSegmentLength() {
        return segmentLength;
    }
}
