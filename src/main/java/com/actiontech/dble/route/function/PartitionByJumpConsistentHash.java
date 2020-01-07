/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.function;


import com.actiontech.dble.config.model.rule.RuleAlgorithm;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.parser.util.PairUtil;
import com.actiontech.dble.util.StringUtil;

/**
 * form one paper of Google
 *
 * @author XiaoSK
 */
public final class PartitionByJumpConsistentHash extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    private static final long UNSIGNED_MASK = 0x7fffffffffffffffL;
    private static final long JUMP = 1L << 31;
    // If JDK >= 1.8, just use Long.parseUnsignedLong("2862933555777941757") instead.
    private static final long CONSTANT = Long.parseLong("286293355577794175", 10) * 10 + 7;

    private int partitionCount;

    private int hashSliceStart = 0;
    /**
     * 0 means str.length(), -1 means str.length()-1
     */
    private int hashSliceEnd = -1;

    @Override
    public Integer calculate(String columnValue) {
        if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
            return 0;
        }
        int start = hashSliceStart >= 0 ? hashSliceStart : columnValue.length() + hashSliceStart;
        int end = hashSliceEnd > 0 ? hashSliceEnd : columnValue.length() + hashSliceEnd;
        return jumpConsistentHash(StringUtil.hash(columnValue, start, end), partitionCount);
    }

    @Override
    public void selfCheck() {
        StringBuffer sb = new StringBuffer();
        if (partitionCount <= 0) {
            sb.append("partitionCount is supported > 0");
        }

        if (sb.length() > 0) {
            throw new RuntimeException(sb.toString());
        }
    }

    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        return new Integer[0];
    }

    @Override
    public int getPartitionNum() {
        int nPartition = this.partitionCount;
        return nPartition;
    }

    public static int jumpConsistentHash(final long key, final int buckets) {
        checkBuckets(buckets);
        long k = key;
        long b = -1;
        long j = 0;

        while (j < buckets) {
            b = j;
            k = k * CONSTANT + 1L;

            j = (long) ((b + 1L) * (JUMP / toDouble((k >>> 33) + 1L)));
        }
        return (int) b;
    }

    private static void checkBuckets(final int buckets) {
        if (buckets < 0) {
            throw new IllegalArgumentException("Buckets cannot be less than 0");
        }
    }

    private static double toDouble(final long n) {
        double d = n & UNSIGNED_MASK;
        if (n < 0) {
            d += 0x1.0p63;
        }
        return d;
    }

    public void setHashSlice(String hashSlice) {
        Pair<Integer, Integer> p = PairUtil.sequenceSlicing(hashSlice);
        hashSliceStart = p.getKey();
        hashSliceEnd = p.getValue();
        propertiesMap.put("hashSlice", hashSlice);
    }

    public void setPartitionCount(int totalBuckets) {
        this.partitionCount = totalBuckets;
        propertiesMap.put("partitionCount", String.valueOf(partitionCount));
    }

    public int getPartitionCount() {
        return partitionCount;
    }
}
