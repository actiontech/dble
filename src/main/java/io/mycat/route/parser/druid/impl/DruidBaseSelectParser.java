package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;

import io.mycat.MycatServer;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.visitor.MySQLItemVisitor;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.route.util.RouterUtil;
import io.mycat.sqlengine.mpp.HavingCols;
import io.mycat.sqlengine.mpp.OrderCol;
import io.mycat.util.StringUtil;

public class DruidBaseSelectParser extends DefaultDruidParser {
	private static HashSet<String> aggregateSet = new HashSet<String>(16, 1);
	static {
		//https://dev.mysql.com/doc/refman/5.7/en/group-by-functions.html
		//SQLAggregateExpr
		aggregateSet.addAll(Arrays.asList(MySqlExprParser.AGGREGATE_FUNCTIONS));
		//SQLMethodInvokeExpr but is Aggregate (GROUP BY) Functions
		aggregateSet.addAll(Arrays.asList("BIT_AND", "BIT_OR", "BIT_XOR", "STD", "STDDEV_POP", "STDDEV_SAMP",
				"VARIANCE", "VAR_POP", "VAR_SAMP"));
	}

	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,
			MycatSchemaStatVisitor visitor) throws SQLNonTransientException {
		return super.visitorParse(schema, rrs, stmt, visitor);
	}

	protected void parseOrderAggGroupMysql(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs,
			MySqlSelectQueryBlock mysqlSelectQuery, TableConfig tc) throws SQLNonTransientException {
		if (mysqlSelectQuery.getOrderBy() != null) {
			rrs.setSqlStatement(stmt);
			rrs.setNeedOptimizer(true);
			return;
		}
		parseAggGroupCommon(schema, stmt, rrs, mysqlSelectQuery, tc);
		if(rrs.isNeedOptimizer()){
			return;
		}
//		// setOrderByCols TODO: ORDER BY has bugs,so it will not reach here. BY YHQ
//		if (mysqlSelectQuery.getOrderBy() != null) {
//			List<SQLSelectOrderByItem> orderByItems = mysqlSelectQuery.getOrderBy().getItems();
//			rrs.setOrderByCols(buildOrderByCols(orderByItems, aliaColumns));
//		}
	}

	private void parseAggExprCommon(SchemaConfig schema, RouteResultset rrs, SQLSelectQueryBlock mysqlSelectQuery, Map<String, String> aliaColumns, TableConfig tc) throws SQLNonTransientException {
		List<SQLSelectItem> selectList = mysqlSelectQuery.getSelectList();
		for (int i = 0; i < selectList.size(); i++) {
			SQLSelectItem item = selectList.get(i);
			SQLExpr itemExpr = item.getExpr();
			if (itemExpr instanceof SQLAggregateExpr) {
				/*
				 * MAX,MIN; SUM,COUNT without distinct is not need optimize, but
				 * there is bugs in default Aggregate IN FACT ,ONLY:
				 * SUM(distinct ),COUNT(distinct),AVG,STDDEV,GROUP_CONCAT
				 */
				rrs.setNeedOptimizer(true);
				return;
			} else if (itemExpr instanceof SQLMethodInvokeExpr) {
				String MethodName = ((SQLMethodInvokeExpr) itemExpr).getMethodName().toUpperCase();
				if (aggregateSet.contains(MethodName)) {
					rrs.setNeedOptimizer(true);
					return;
				} else {
					if (isSumFunc(schema.getName(), itemExpr)) {
						rrs.setNeedOptimizer(true);
						return;
					} else {
						addToAliaColumn(aliaColumns, item);
					}
				}
			} else if (itemExpr instanceof SQLAllColumnExpr) {
				continue;
			} else {
				if (isSumFunc(schema.getName(), itemExpr)) {
					rrs.setNeedOptimizer(true);
					return;
				} else {
					addToAliaColumn(aliaColumns, item);
				}
			}
		}
		if (mysqlSelectQuery.getGroupBy() != null) {
			SQLSelectGroupByClause groupBy = mysqlSelectQuery.getGroupBy();
			boolean hasPartitionColumn = false;

			if (groupBy.getHaving() != null) {
				//TODO:DEFAULT HAVING HAS BUG,So NeedOptimizer 
				//SEE DataNodeMergeManager.java function onRowMetaData
				rrs.setNeedOptimizer(true);
				return;
			}
			for (SQLExpr groupByItem : groupBy.getItems()) {
				if (isNeedOptimizer(groupByItem)) {
					rrs.setNeedOptimizer(true);
					return;
				} else if (groupByItem instanceof SQLIdentifierExpr) {
					SQLIdentifierExpr item = (SQLIdentifierExpr) groupByItem;
					if (item.getSimpleName().equalsIgnoreCase(tc.getPartitionColumn())) {
						hasPartitionColumn = true;
					}
				} else if (groupByItem instanceof SQLPropertyExpr) {
					SQLPropertyExpr item = (SQLPropertyExpr) groupByItem;
					if (item.getSimpleName().equalsIgnoreCase(tc.getPartitionColumn())) {
						hasPartitionColumn = true;
					}
				}
			}
			if (hasPartitionColumn == false) {
				rrs.setNeedOptimizer(true);
				return;
			}
		}
	}
	private boolean isSumFunc(String schema,SQLExpr itemExpr){
		MySQLItemVisitor ev = new MySQLItemVisitor(schema);
		itemExpr.accept(ev);
		Item selItem = ev.getItem();
		return contactSumFunc(selItem);
	}
	private boolean contactSumFunc(Item selItem){
		if(selItem.withSumFunc){
			return true;
		}
		if(selItem.getArgCount()>0){
			for(Item child:selItem.arguments()){
				if(contactSumFunc(child)){
					return true;
				}
			}
			return false;
		}
		else{
			return false;
		}
	}
	private boolean isNeedOptimizer(SQLExpr expr){
		// it is NotSimpleColumn TODO: 细分是否真的NeedOptimizer
		return !(expr instanceof SQLPropertyExpr) && !(expr instanceof SQLIdentifierExpr);
	}
	private void addToAliaColumn(Map<String, String> aliaColumns, SQLSelectItem item) {
		String alia = item.getAlias();
		String field = getFieldName(item);
		if (alia == null) {
			alia = field;
		}
		aliaColumns.put(field, alia);
	}
	
	protected Map<String, String> parseAggGroupCommon(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs,
			SQLSelectQueryBlock mysqlSelectQuery, TableConfig tc) throws SQLNonTransientException {
		Map<String, String> aliaColumns = new HashMap<String, String>();
		Map<String, Integer> aggrColumns = new HashMap<String, Integer>();
		List<String> havingColsName = new ArrayList<String>();
		
		parseAggExprCommon(schema, rrs, mysqlSelectQuery, aliaColumns, tc);
		if (rrs.isNeedOptimizer()) {
			rrs.setSqlStatement(stmt);
			return aliaColumns;
		}
		
		if (aggrColumns.size() > 0) {
			rrs.setMergeCols(aggrColumns);
		}

		// 通过优化转换成group by来实现
		boolean isNeedChangeSql = (mysqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCT)||(mysqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCTROW);
		if (isNeedChangeSql) {
			mysqlSelectQuery.setDistionOption(0);
			SQLSelectGroupByClause groupBy = new SQLSelectGroupByClause();
			for (String fieldName : aliaColumns.keySet()) {
				groupBy.addItem(new SQLIdentifierExpr(fieldName));
			}
			mysqlSelectQuery.setGroupBy(groupBy);
		}

		// setGroupByCols
		if (mysqlSelectQuery.getGroupBy() != null) {
			List<SQLExpr> groupByItems = mysqlSelectQuery.getGroupBy().getItems();
			String[] groupByCols = buildGroupByCols(groupByItems, aliaColumns);
			rrs.setGroupByCols(groupByCols);
			rrs.setHavings(buildGroupByHaving(mysqlSelectQuery.getGroupBy().getHaving()));
			rrs.setHasAggrColumn(true);
			rrs.setHavingColsName(havingColsName.toArray()); 
		}

		if (isNeedChangeSql) {
			rrs.changeNodeSqlAfterAddLimit(schema, stmt.toString(), 0, -1);
		}
		return aliaColumns;
	}

	private HavingCols buildGroupByHaving(SQLExpr having) {
		if (having == null) {
			return null;
		}

		SQLBinaryOpExpr expr = ((SQLBinaryOpExpr) having);
		SQLExpr left = expr.getLeft();
		SQLBinaryOperator operator = expr.getOperator();
		SQLExpr right = expr.getRight();

		String leftValue = null;
		if (left instanceof SQLAggregateExpr) {
			leftValue = ((SQLAggregateExpr) left).getMethodName() + "("
					+ ((SQLAggregateExpr) left).getArguments().get(0) + ")";
		} else if (left instanceof SQLIdentifierExpr) {
			leftValue = ((SQLIdentifierExpr) left).getName();
		}

		String rightValue = null;
		if (right instanceof SQLNumericLiteralExpr) {
			rightValue = right.toString();
		} else if (right instanceof SQLTextLiteralExpr) {
			rightValue = StringUtil.removeApostrophe(right.toString());
		}

		return new HavingCols(leftValue, rightValue, operator.getName());
	}

	protected boolean isRoutMultiNode(SchemaConfig schema, RouteResultset rrs) {
		if (rrs.getNodes() != null && rrs.getNodes().length > 1) {
			return true;
		}
		LayerCachePool tableId2DataNodeCache = (LayerCachePool) MycatServer.getInstance().getCacheService()
				.getCachePool("TableID2DataNodeCache");
		try {
			tryRouteSingleTable(schema, rrs, tableId2DataNodeCache);
			if (rrs.getNodes() != null && rrs.getNodes().length > 1) {
				return true;
			}
		} catch (SQLNonTransientException e) {
			throw new RuntimeException(e);
		}
		return false;
	}
	private String getFieldName(SQLSelectItem item) {
		if ((item.getExpr() instanceof SQLPropertyExpr) || (item.getExpr() instanceof SQLMethodInvokeExpr)
				|| (item.getExpr() instanceof SQLIdentifierExpr) || item.getExpr() instanceof SQLBinaryOpExpr) {
			return item.getExpr().toString();// 字段别名
		} else {
			return item.toString();
		}
	}

	
	
	
	protected void tryRouteSingleTable(SchemaConfig schema, RouteResultset rrs, LayerCachePool cachePool)
			throws SQLNonTransientException {
		if (rrs.isFinishedRoute()) {
			return;// 避免重复路由
		}
		SortedSet<RouteResultsetNode> nodeSet = new TreeSet<RouteResultsetNode>();
		String table = ctx.getTables().get(0);
		if (RouterUtil.isNoSharding(schema, table)) {
			rrs = RouterUtil.routeToSingleNode(rrs, schema.getDataNode());
			return;
		}
		for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
			RouteResultset rrsTmp = RouterUtil.tryRouteForOneTable(schema, ctx, unit, table, rrs, true, cachePool);
			if (rrsTmp != null && rrsTmp.getNodes() != null) {
				for (RouteResultsetNode node : rrsTmp.getNodes()) {
					nodeSet.add(node);
				}
			}
		}
		if (nodeSet.size() == 0) {
			String msg = " find no Route:" + rrs.getStatement();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}

		RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
		int i = 0;
		for (Iterator<RouteResultsetNode> iterator = nodeSet.iterator(); iterator.hasNext();) {
			nodes[i] = (RouteResultsetNode) iterator.next();
			i++;

		}

		rrs.setNodes(nodes);
		rrs.setFinishedRoute(true);
	}
	protected String getAliaColumn(Map<String, String> aliaColumns, String column) {
		String alia = aliaColumns.get(column);
		if (alia == null) {
			if (column.indexOf(".") < 0) {
				String col = "." + column;
				String col2 = ".`" + column + "`";
				// 展开aliaColumns，将<c.name,cname>之类的键值对展开成<c.name,cname>和<name,cname>
				for (Map.Entry<String, String> entry : aliaColumns.entrySet()) {
					if (entry.getKey().endsWith(col) || entry.getKey().endsWith(col2)) {
						if (entry.getValue() != null && entry.getValue().indexOf(".") > 0) {
							return column;
						}
						return entry.getValue();
					}
				}
			}

			return column;
		} else {
			return alia;
		}
	}

	private String[] buildGroupByCols(List<SQLExpr> groupByItems, Map<String, String> aliaColumns) {
		String[] groupByCols = new String[groupByItems.size()];
		for (int i = 0; i < groupByItems.size(); i++) {
			SQLExpr sqlExpr = groupByItems.get(i);
			String column = null;
			if (sqlExpr instanceof SQLIdentifierExpr) {
				column = ((SQLIdentifierExpr) sqlExpr).getName();
			} else if (sqlExpr instanceof SQLMethodInvokeExpr) {
				column = ((SQLMethodInvokeExpr) sqlExpr).toString();
			} else if (sqlExpr instanceof MySqlOrderingExpr) {
				// todo czn
				SQLExpr expr = ((MySqlOrderingExpr) sqlExpr).getExpr();

				if (expr instanceof SQLName) {
					column = StringUtil.removeBackQuote(((SQLName) expr).getSimpleName());
				} else {
					column = StringUtil.removeBackQuote(expr.toString());
				}
			} else if (sqlExpr instanceof SQLPropertyExpr) {
				/**
				 * 针对子查询别名，例如select id from (select h.id from hotnews h union
				 * select h.title from hotnews h ) as t1 group by t1.id;
				 */
				column = sqlExpr.toString();
			}
			if (column == null) {
				column = sqlExpr.toString();
			}
			int dotIndex = column.indexOf(".");
			int bracketIndex = column.indexOf("(");
			// 通过判断含有括号来决定是否为函数列
			if (dotIndex != -1 && bracketIndex == -1) {
				// 此步骤得到的column必须是不带.的，有别名的用别名，无别名的用字段名
				column = column.substring(dotIndex + 1);
			}
			groupByCols[i] = getAliaColumn(aliaColumns, column);// column;
		}
		return groupByCols;
	}

	protected LinkedHashMap<String, Integer> buildOrderByCols(List<SQLSelectOrderByItem> orderByItems,
			Map<String, String> aliaColumns) {
		LinkedHashMap<String, Integer> map = new LinkedHashMap<String, Integer>();
		for (int i = 0; i < orderByItems.size(); i++) {
			SQLOrderingSpecification type = orderByItems.get(i).getType();
			// orderColumn只记录字段名称,因为返回的结果集是不带表名的。
			SQLExpr expr = orderByItems.get(i).getExpr();
			String col;
			if (expr instanceof SQLName) {
				col = ((SQLName) expr).getSimpleName();
			} else {
				col = expr.toString();
			}
			if (type == null) {
				type = SQLOrderingSpecification.ASC;
			}
			col = getAliaColumn(aliaColumns, col);// 此步骤得到的col必须是不带.的，有别名的用别名，无别名的用字段名
			map.put(col,
					type == SQLOrderingSpecification.ASC ? OrderCol.COL_ORDER_TYPE_ASC : OrderCol.COL_ORDER_TYPE_DESC);
		}
		return map;
	}
}
