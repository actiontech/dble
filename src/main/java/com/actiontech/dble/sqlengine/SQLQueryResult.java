/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.sqlengine;

public class SQLQueryResult<T> {
    private final T result;
    private final boolean success;

    private final String shardingNode;    // shardingNode or database name
    private String tableName;

    public SQLQueryResult(T result, boolean success) {
        super();
        this.result = result;
        this.success = success;
        this.shardingNode = null;
    }

    public SQLQueryResult(T result, boolean success, String shardingNode) {
        super();
        this.result = result;
        this.success = success;
        this.shardingNode = shardingNode;
    }

    public T getResult() {
        return result;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getShardingNode() {
        return shardingNode;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
