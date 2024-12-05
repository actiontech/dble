/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import org.apache.commons.lang.StringUtils;

public abstract class ItemIdent extends Item {
    /*
     * We have to store initial values of db_name, table_name and field_name to
     * be able to restore them during cleanup() because they can be updated
     * during fix_fields() to values from Field object and life-time of those is
     * shorter than life-time of Item_field.
     */

    protected String dbName;
    protected String tableName;

    public ItemIdent(final String dbNameArg, final String tableNameArg, final String fieldNameArg) {
        String tempTableName;
        String tempDbName;
        if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            tempTableName = tableNameArg == null ? null : tableNameArg.toLowerCase();
            tempDbName = dbNameArg == null ? null : dbNameArg.toLowerCase();
        } else {
            tempTableName = tableNameArg;
            tempDbName = dbNameArg;
        }

        this.dbName = tempDbName;
        this.tableName = tempTableName;
        this.itemName = fieldNameArg;
        this.withUnValAble = true;
    }

    @Override
    public boolean isWild() {
        return StringUtils.equalsIgnoreCase(itemName, "*");
    }

    @Override
    public String toString() {
        return toExpression().toString();
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
