package com.actiontech.dble.meta.table;

import java.util.HashSet;
import java.util.Set;

public abstract class ModeTableHandler {
    private volatile Set<String> shardDNSet = new HashSet<>();

    abstract boolean loadMetaData();

    abstract void handleTable(String table, String shardingNode, boolean isView, String sql);

    // node countdown
    abstract void countdown(String shardingNode, Set<String> remainingTables);

    // last node
    abstract void tryComplete(String shardingNode, boolean isLastShardingNode);


    public boolean isComplete() {
        return shardDNSet.size() == 0;
    }

    public boolean isLastShardingNode(String shardingNode) {
        synchronized (shardDNSet) {
            shardDNSet.remove(shardingNode);
            return shardDNSet.size() == 0;
        }
    }

    public Set<String> getShardDNSet() {
        return shardDNSet;
    }
}
