package com.actiontech.dble.meta;

import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLNotNullConstraint;

import java.util.List;

public final class TableMeta {

    private String tableName;
    private long version;
    private String createSql;
    private List<ColumnMeta> columns;

    public TableMeta() {
    }

    public TableMeta(TableMeta origin, long newVersion) {
        tableName = origin.getTableName();
        columns = origin.getColumns();
        createSql = origin.getCreateSql();
        version = newVersion;
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
        return tableName.equals(temp.getTableName()) && version == temp.getVersion() &&
                createSql.equals(temp.getCreateSql()) && columns.equals(temp.getColumns());
    }

    @Override
    public int hashCode() {
        int result = 31 + tableName.hashCode();
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + createSql.hashCode();
        result = 31 * result + columns.hashCode();

        return result;
    }

    public static class ColumnMeta {
        private String name;
        private String dataType;
        private boolean canNull = true;
        private String defaultVal;

        public ColumnMeta(SQLColumnDefinition def) {
            this.name = StringUtil.removeBackAndDoubleQuote(def.getName().getSimpleName());
            this.dataType = def.getDataType().getName();
            for (SQLColumnConstraint constraint : def.getConstraints()) {
                if (constraint instanceof SQLNotNullConstraint) {
                    this.canNull = false;
                    break;
                }
            }
            this.defaultVal = null == def.getDefaultExpr() ? null : StringUtil.removeApostrophe(def.getDefaultExpr().toString());
        }

        public String getName() {
            return name;
        }

        public String getDataType() {
            return dataType;
        }

        public boolean isCanNull() {
            return canNull;
        }

        public String getDefaultVal() {
            return defaultVal;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ColumnMeta)) {
                return false;
            }
            ColumnMeta temp = (ColumnMeta) other;
            return name.equalsIgnoreCase(temp.getName()) && dataType.equals(temp.getDataType()) &&
                    canNull == temp.isCanNull();
        }

        @Override
        public int hashCode() {
            int result = 31 + name.hashCode();
            result = 31 * result + dataType.hashCode();
            result = 31 * result + (canNull ? 1231 : 1237);

            return result;
        }
    }

}
