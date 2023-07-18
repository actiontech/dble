/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.backend.datasource;

public class ShardingNode extends BaseNode {

    public ShardingNode(String dbGroupName, String hostName, String database, PhysicalDbGroup dbGroup) {
        super(dbGroupName, hostName, database, dbGroup);
        nodeType = "sharding";
    }
}
