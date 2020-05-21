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

    public static class ColumnMeta {
        private String name;
        private String dataType;
        private boolean canNull = true;

        public ColumnMeta(SQLColumnDefinition def) {
            this.name = StringUtil.removeBackAndDoubleQuote(def.getName().getSimpleName());
            this.dataType = def.getDataType().getName();
            for (SQLColumnConstraint constraint : def.getConstraints()) {
                if (constraint instanceof SQLNotNullConstraint) {
                    this.canNull = false;
                    break;
                }
            }
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
    }

}
