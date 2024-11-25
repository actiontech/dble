/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.sqlengine;

public class SQLQueryResult<T> {
    private final T result;
    private final boolean success;

    private final String shardingNode;    // shardingNode or database name
    private String tableName;
    private final boolean zeroRow;

    public SQLQueryResult(T result, boolean success, boolean zeroRow) {
        super();
        this.result = result;
        this.success = success;
        this.shardingNode = null;
        this.zeroRow = zeroRow;
    }

    public SQLQueryResult(T result, boolean success, String shardingNode, boolean zeroRow) {
        super();
        this.result = result;
        this.success = success;
        this.shardingNode = shardingNode;
        this.zeroRow = zeroRow;
    }

    public boolean isZeroRow() {
        return zeroRow;
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
