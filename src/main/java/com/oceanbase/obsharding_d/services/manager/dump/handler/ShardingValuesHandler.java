/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.dump.handler;

import com.oceanbase.obsharding_d.config.model.sharding.table.ShardingTableConfig;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.services.manager.dump.DumpFileContext;
import com.oceanbase.obsharding_d.route.function.AbstractPartitionAlgorithm;
import com.oceanbase.obsharding_d.services.manager.dump.parse.InsertQueryPos;
import com.oceanbase.obsharding_d.singleton.SequenceManager;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.sql.SQLNonTransientException;
import java.util.List;

public class ShardingValuesHandler extends DefaultValuesHandler {

    @Override
    public void process(DumpFileContext context, InsertQueryPos insertQueryPos, List<Pair<Integer, Integer>> valuePair) throws SQLNonTransientException {
        Integer nodeIndex = handleShardingColumn(context, insertQueryPos, valuePair);
        String shardingNode = context.getTableConfig().getShardingNodes().get(nodeIndex);
        long incrementColumnVal = 0;
        if (context.getIncrementColumnIndex() != -1) {
            String tableKey = StringUtil.getFullName(context.getSchema(), context.getTable());
            incrementColumnVal = SequenceManager.nextId(tableKey, null);
        }
        context.getWriter().writeInsertHeader(shardingNode, new InsertQuery(insertQueryPos, valuePair, context.getIncrementColumnIndex(), incrementColumnVal));
    }

    private Integer handleShardingColumn(DumpFileContext context, InsertQueryPos insertQueryPos, List<Pair<Integer, Integer>> valuePair) throws SQLNonTransientException {
        AbstractPartitionAlgorithm algorithm = ((ShardingTableConfig) context.getTableConfig()).getFunction();
        Pair<Integer, Integer> pair = valuePair.get(context.getPartitionColumnIndex());
        String shardingValue = getValueString(insertQueryPos.getInsertChars(), pair);

        Integer nodeIndex;
        try {
            nodeIndex = algorithm.calculate(shardingValue);
            // null means can't find any valid index
            if (nodeIndex == null || nodeIndex >= context.getTableConfig().getShardingNodes().size()) {
                throw new SQLNonTransientException("can't find any valid shardingnode shardingValue:" + shardingValue);
            }
        } catch (Exception e) {
            throw new SQLNonTransientException("can't calculate valid shardingnode shardingValue,due to " + e.getMessage());
        }
        return nodeIndex;
    }

    private String getValueString(char[] src, Pair<Integer, Integer> range) {
        StringBuilder target = new StringBuilder();
        int start = range.getKey();
        int end = range.getValue();
        if (src[start] == '\'' && src[end - 1] == '\'') {
            start++;
            end--;
        }
        for (int i = start; i < end; i++) {
            target.append(src[i]);
        }
        return target.toString();
    }

    public static class InsertQuery {
        private final InsertQueryPos insertQueryPos;
        private final List<Pair<Integer, Integer>> valuePair;
        private final int incrementColumnIndex;
        private final long incrementColumnValue;

        public InsertQuery(InsertQueryPos insertQueryPos, List<Pair<Integer, Integer>> valuePair, int incrementColumnIndex, long incrementColumnValue) {
            this.insertQueryPos = insertQueryPos;
            this.valuePair = valuePair;
            this.incrementColumnIndex = incrementColumnIndex;
            this.incrementColumnValue = incrementColumnValue;
        }

        public long getIncrementColumnValue() {
            return incrementColumnValue;
        }

        public int getIncrementColumnIndex() {
            return incrementColumnIndex;
        }

        public InsertQueryPos getInsertQueryPos() {
            return insertQueryPos;
        }

        public List<Pair<Integer, Integer>> getValuePair() {
            return valuePair;
        }
    }
}
