package com.actiontech.dble.meta;

import java.util.List;

public final class TableMeta {

    private int id;
    private String schemaName;
    private String tableName;
    private long version;
    private String createSql;
    private List<ColumnMeta> columns;

    public TableMeta() {
    }

    public TableMeta(TableMeta origin, long newVersion) {
        tableName = origin.getTableName();
        this.schemaName = origin.getSchemaName();
        columns = origin.getColumns();
        createSql = origin.getCreateSql();
        version = newVersion;
    }

    public TableMeta(int id, String schemaName, String tableName) {
        this.id = id;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public List<ColumnMeta> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMeta> columns) {
        this.columns = columns;
    }

    public String getCreateSql() {
        return createSql;
    }

    public void setCreateSql(String createSql) {
        this.createSql = createSql;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TableMeta)) {
            return false;
        }

        TableMeta temp = (TableMeta) other;
        return id == temp.getId() && schemaName.equals(temp.getSchemaName()) && tableName.equals(temp.getTableName()) &&
                version == temp.getVersion() && createSql.equals(temp.getCreateSql()) && columns.equals(temp.getColumns());
    }

    @Override
    public int hashCode() {
        int result = 31 + tableName.hashCode();
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + createSql.hashCode();
        result = 31 * result + columns.hashCode();
        result = 31 * result + Integer.hashCode(id);
        if (null != schemaName) {
            result = 31 * result + schemaName.hashCode();
        }

        return result;
    }

}
