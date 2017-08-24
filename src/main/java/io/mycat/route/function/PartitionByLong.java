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
package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;
import io.mycat.route.util.PartitionUtil;

public final class PartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final long serialVersionUID = -4712399083043025898L;
    protected int[] count;
    protected int[] length;
    protected PartitionUtil partitionUtil;

    private static int[] toIntArray(String string) {
        String[] strs = io.mycat.util.SplitUtil.split(string, ',', true);
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
        int length = partitionUtil.getPartitionLength();
        if (end - begin >= length || begin > end) { //TODO: optimize begin > end
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
