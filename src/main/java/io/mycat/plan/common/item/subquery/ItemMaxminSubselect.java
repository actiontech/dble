/**
 * 
 */
package io.mycat.plan.common.item.subquery;

import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

public class ItemMaxminSubselect extends ItemSinglerowSubselect {
	private boolean max;

	/**
	 * @param currentDb
	 * @param query
	 */
	public ItemMaxminSubselect(String currentDb, SQLSelectQuery query, boolean max) {
		super(currentDb, query);
		this.max = max;
	}

}
