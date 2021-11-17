package com.actiontech.dble.services.manager.dump.handler;

import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.services.manager.dump.DumpFileContext;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.services.manager.dump.parse.InsertQueryPos;
import com.actiontech.dble.singleton.SequenceManager;
import com.actiontech.dble.util.StringUtil;

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
            incrementColumnVal = SequenceManager.getHandler().nextId(tableKey);
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
                throw new SQLNonTransientException("can't find any valid shardingnode shardingValue" + shardingValue);
            }
        } catch (Exception e) {
            throw new SQLNonTransientException("can't calculate valid shardingnode shardingValue" + shardingValue + ",due to " + e.getMessage());
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
