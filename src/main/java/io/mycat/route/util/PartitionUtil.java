/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.route.util;

import io.mycat.util.StringUtil;

import java.io.Serializable;

/**
 * 数据分区工具
 *
 * @author mycat
 */
public final class PartitionUtil implements Serializable {

    // 分区最大长度:数据段分布定义，如果取模的数是2^n， 使用x % 2^n == x & (2^n - 1)等式，来优化性能。
    private static final int MAX_PARTITION_LENGTH = 2880;
    private int partitionLength;

    // %转换为&操作的换算数值
    private long addValue;

    // 分区线段
    private int[] segment;

    private boolean canProfile = false;

    private int segmentLength = 0;

    /**
     * <pre>
     * @param count 表示定义的分区数
     * @param length 表示对应每个分区的取值长度
     * 注意：其中count,length两个数组的长度必须是一致的。
     * 约束：MAX_PARTITION_LENGTH >=sum((count[i]*length[i]))
     * </pre>
     */
    public PartitionUtil(int[] count, int[] length) {
        if (count == null || length == null || (count.length != length.length)) {
            throw new RuntimeException("error,check your scope & scopeLength definition.");
        }
        for (int i = 0; i < count.length; i++) {
            if (count[i] <= 0) {
                throw new RuntimeException("error,check your scope at least 1.");
            }
            segmentLength += count[i];
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

        // 数据映射操作
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