/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.sharding.table;

public class ERTable {
    private final String table;
    private final String column;
    private final String schema;
    private int hashCode = 0;

    public ERTable(String schema, String table, String column) {
        if (schema == null)
            throw new IllegalArgumentException("ERTable's schema can't be null");
        this.schema = schema;
        if (table == null)
            throw new IllegalArgumentException("ERTable's tableName can't be null");
        this.table = table;
        if (column == null)
            throw new IllegalArgumentException("ERTable's column can't be null");
        this.column = column;
    }

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    public String getSchema() {
        return schema;
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            final int constant = 37;
            int hash = 17;
            hash += constant * (schema == null ? 0 : schema.hashCode());
            hash += constant * (table == null ? 0 : table.hashCode());
            hash += constant * (column == null ? 0 : column.toLowerCase().hashCode());
            hashCode = hash;
        }
        return hashCode;
    }


    public ERTable changeToLowerCase() {
        return new ERTable(schema.toLowerCase(), table.toLowerCase(), column);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj instanceof ERTable) {
            ERTable erTable = (ERTable) obj;
            return this.schema.equals(erTable.getSchema()) &&
                    this.table.equals(erTable.getTable()) &&
                    this.column.equalsIgnoreCase(erTable.getColumn());
        }
        return false;
    }
}
