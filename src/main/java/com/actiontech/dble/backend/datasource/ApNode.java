package com.actiontech.dble.backend.datasource;

public class ApNode extends ShardingNode {

    public ApNode(String dbGroupName, String hostName, String database, PhysicalDbGroup dbGroup) {
        super(dbGroupName, hostName, database, dbGroup);
    }
}
