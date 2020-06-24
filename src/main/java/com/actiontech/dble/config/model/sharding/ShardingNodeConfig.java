/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.model.sharding;

public final class ShardingNodeConfig {

    private final String name;
    private final String database;
    private final String dbGroupName;

    public ShardingNodeConfig(String name, String database, String dbGroupName) {
        super();
        this.name = name;
        this.database = database;
        this.dbGroupName = dbGroupName;
    }

    public String getName() {
        return name;
    }

    public String getDatabase() {
        return database;
    }

    public String getDbGroupName() {
        return dbGroupName;
    }

}
