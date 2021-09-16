package com.actiontech.dble.meta.table;

import java.util.HashSet;
import java.util.Set;

public abstract class ModeTableHandler {
    private volatile Set<String> shardDNSet = new HashSet<>();

    abstract boolean loadMetaData();

    public boolean isComplete() {
        return shardDNSet.size() == 0;
    }

    public synchronized boolean isLastShardingNode(String shardingNode) {
        shardDNSet.remove(shardingNode);
        return shardDNSet.size() == 0;
    }

    public Set<String> getShardDNSet() {
        return shardDNSet;
    }
}
