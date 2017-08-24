package io.mycat.plan.common.item;

import org.apache.commons.lang.StringUtils;

public abstract class ItemIdent extends Item {
//    /*
//     * We have to store initial values of db_name, table_name and field_name to
//     * be able to restore them during cleanup() because they can be updated
//     * during fix_fields() to values from Field object and life-time of those is
//     * shorter than life-time of Item_field.
//     */
//    protected String origDbName;
//    protected String origTableName;
//    protected String origFieldName;

    public String dbName;
    public String tableName;

    public ItemIdent(final String dbNameArg, final String tableNameArg, final String fieldNameArg) {
        this.dbName = dbNameArg;
        this.tableName = tableNameArg;
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

}
