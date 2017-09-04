/**
 *
 */
package com.actiontech.dble.plan.common.item.subquery;

import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

public class ItemMaxminSubselect extends ItemSinglerowSubselect {

    /**
     * @param currentDb
     * @param query
     */
    public ItemMaxminSubselect(String currentDb, SQLSelectQuery query, boolean max) {
        super(currentDb, query);
        boolean max1 = max;
    }

}
