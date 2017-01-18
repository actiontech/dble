package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.nio.handler.FetchStoreNodeOfChildTableHandler;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.meta.protocol.MyCatMeta.TableMeta;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.interceptor.impl.GlobalTableUtil;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;
import io.mycat.sqlengine.mpp.ColumnRoutePair;
import io.mycat.util.StringUtil;

public class DruidInsertParser extends DefaultDruidParser {
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) throws SQLNonTransientException {
		
	}
	
	/**
	 * 考虑因素：isChildTable、批量、是否分片
	 */
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
			throws SQLNonTransientException {
		MySqlInsertStatement insert = (MySqlInsertStatement) stmt;
		String schemaName = schema == null ? null : schema.getName();
		SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(schemaName, insert.getTableSource());
		if (schemaInfo == null) {
			String msg = "No MyCAT Database selected Or Define";
			throw new SQLNonTransientException(msg);
		}
		if (parserNoSharding(schemaName, schemaInfo, rrs, insert)) {
			return;
		}
		schema = schemaInfo.schemaConfig;
		String tableName = schemaInfo.table;
		insert.setTableSource(new SQLIdentifierExpr(tableName));
		ctx.addTable(tableName);
		// 整个schema都不分库或者该表不拆分
		TableConfig tc = schema.getTables().get(tableName);
		if (tc == null) {
			String msg = "can't find table [" + tableName + "] define in schema:" + schema.getName();
			throw new SQLNonTransientException(msg);
		}
		if (GlobalTableUtil.useGlobleTableCheck() && tc.isGlobalTable()) {
			String sql = convertInsertSQL(schemaInfo, insert);
			rrs.setStatement(sql);
			RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), sql, tc.isGlobalTable());
			rrs.setFinishedRoute(true);
			return;
		}
		// childTable的insert直接在解析过程中完成路由
		if (tc.isChildTable()) {
			parserChildTable(schemaInfo, rrs, insert);
			return;
		}
		String partitionColumn = tc.getPartitionColumn();
		if (partitionColumn != null) {
			// 批量insert
			if (isMultiInsert(insert)) {
				parserBatchInsert(schemaInfo, rrs, partitionColumn, insert);
			} else {
				parserSingleInsert(schemaInfo, rrs, partitionColumn, insert);
			}

		}
	}
	
	private boolean parserNoSharding(String schemaName, SchemaInfo schemaInfo, RouteResultset rrs, MySqlInsertStatement insert) {
		if (RouterUtil.isNoSharding(schemaInfo.schemaConfig, schemaInfo.table)) {
			if (insert.getQuery() != null) {
				//TODO:TABLES insert.getQuery() are all NoSharding
				return false;
			}
			RouterUtil.routeForTableMeta(rrs, schemaInfo.schemaConfig, schemaInfo.table, rrs.getStatement());
			rrs.setFinishedRoute(true);
			return true;
		}
		return false;
	}
	/**
	 * 是否为批量插入：insert into ...values (),()...或 insert into ...select.....
	 * @param insertStmt
	 * @return
	 */
	private boolean isMultiInsert(MySqlInsertStatement insertStmt) {
		return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1) || insertStmt.getQuery() != null;
	}
	
	private RouteResultset parserChildTable(SchemaInfo schemaInfo, RouteResultset rrs, MySqlInsertStatement insertStmt) throws SQLNonTransientException {
		SchemaConfig schema = schemaInfo.schemaConfig;
		String tableName = schemaInfo.table;
		TableConfig tc = schema.getTables().get(tableName);
		if (isMultiInsert(insertStmt)) {
			String msg = "ChildTable multi insert not provided";
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		String joinKey = tc.getJoinKey();
		int joinKeyIndex = getJoinKeyIndex(schemaInfo, insertStmt, joinKey);
		String joinKeyVal = insertStmt.getValues().getValues().get(joinKeyIndex).toString();
		String realVal = joinKeyVal;
		if (joinKeyVal.startsWith("'") && joinKeyVal.endsWith("'") && joinKeyVal.length() > 2) {
			realVal = joinKeyVal.substring(1, joinKeyVal.length() - 1);
		}
		String sql = insertStmt.toString();
		// try to route by ER parent partion key
		RouteResultset theRrs = routeByERParentKey(sql, rrs, tc, realVal);
		if (theRrs != null) {
			rrs.setFinishedRoute(true);
			return theRrs;
		}
		// route by sql query root parent's datanode
		String findRootTBSql = tc.getLocateRTableKeySql().toLowerCase() + realVal;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find root parent's node sql " + findRootTBSql);
		}
		FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler(findRootTBSql, rrs.getSession());
		String dn = fetchHandler.execute(schema.getName(), tc.getRootParent().getDataNodes());
		if (dn == null) {
			throw new SQLNonTransientException("can't find (root) parent sharding node for sql:" + sql);
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("found partion node for child table to insert " + dn + " sql :" + sql);
		}
		return RouterUtil.routeToSingleNode(rrs, dn, sql);
	}

	private RouteResultset routeByERParentKey(String stmt, RouteResultset rrs, TableConfig tc, String joinKeyVal)
			throws SQLNonTransientException {
		// only has one parent level and ER parent key is parent table's partition key
		if (tc.isSecondLevel() && tc.getParentTC().getPartitionColumn().equals(tc.getParentKey())) {
			Set<ColumnRoutePair> parentColVal = new HashSet<ColumnRoutePair>(1);
			ColumnRoutePair pair = new ColumnRoutePair(joinKeyVal);
			parentColVal.add(pair);
			Set<String> dataNodeSet = RouterUtil.ruleCalculate(tc.getParentTC(), parentColVal);
			if (dataNodeSet.isEmpty() || dataNodeSet.size() > 1) {
				throw new SQLNonTransientException("parent key can't find  valid datanode ,expect 1 but found: " + dataNodeSet.size());
			}
			String dn = dataNodeSet.iterator().next();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  " + dn + " sql :" + stmt);
			}
			return RouterUtil.routeToSingleNode(rrs, dn, stmt);
		}
		return null;
	}
	/**
	 * 单条insert（非批量）
	 * @param schema
	 * @param rrs
	 * @param partitionColumn
	 * @param tableName
	 * @param insertStmt
	 * @throws SQLNonTransientException
	 */
	private void parserSingleInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn, MySqlInsertStatement insertStmt) throws SQLNonTransientException {
		int shardingColIndex = getShardingColIndex(schemaInfo, insertStmt, partitionColumn);
		SQLExpr valueExpr = insertStmt.getValues().getValues().get(shardingColIndex);
		String shardingValue = null;
		if(valueExpr instanceof SQLIntegerExpr) {
			SQLIntegerExpr intExpr = (SQLIntegerExpr)valueExpr;
			shardingValue = intExpr.getNumber() + "";
		} else if (valueExpr instanceof SQLCharExpr) {
			SQLCharExpr charExpr = (SQLCharExpr)valueExpr;
			shardingValue = charExpr.getText();
		}
		TableConfig tableConfig = schemaInfo.schemaConfig.getTables().get(schemaInfo.table);
		AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
		Integer nodeIndex = algorithm.calculate(shardingValue);
		//没找到插入的分片
		if(nodeIndex == null) {
			String msg = "can't find any valid datanode :" + schemaInfo.table
					+ " -> " + partitionColumn + " -> " + shardingValue;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		RouteResultsetNode[] nodes = new RouteResultsetNode[1];
		nodes[0] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex), rrs.getSqlType(), insertStmt.toString());
		nodes[0].setSource(rrs);

		// insert into .... on duplicateKey 
		//such as :INSERT INTO TABLEName (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE b=VALUES(b); 
		//INSERT INTO TABLEName (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE c=c+1;
		if(insertStmt.getDuplicateKeyUpdate() != null) {
			List<SQLExpr> updateList = insertStmt.getDuplicateKeyUpdate();
			for(SQLExpr expr : updateList) {
				SQLBinaryOpExpr opExpr = (SQLBinaryOpExpr)expr;
				String column = StringUtil.removeBackquote(opExpr.getLeft().toString().toUpperCase());
				if(column.equals(partitionColumn)) {
					String msg = "Sharding column can't be updated: " + schemaInfo.table + " -> " + partitionColumn;
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}
			}
		}
		rrs.setNodes(nodes);
		rrs.setFinishedRoute(true);
	}
	
	/**
	 * insert into .... select .... 或insert into table() values (),(),....
	 *
	 * @param schema
	 * @param rrs
	 * @param insertStmt
	 * @throws SQLNonTransientException
	 */
	private void parserBatchInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
			MySqlInsertStatement insertStmt) throws SQLNonTransientException {
		// insert into table() values (),(),....
		if (insertStmt.getValuesList().size() > 1) {
			SchemaConfig schema = schemaInfo.schemaConfig;
			String tableName = schemaInfo.table;
			// 字段列数
			int columnNum = getTableColumns(schemaInfo, insertStmt);
			int shardingColIndex = getShardingColIndex(schemaInfo, insertStmt, partitionColumn);
			List<ValuesClause> valueClauseList = insertStmt.getValuesList();
			Map<Integer, List<ValuesClause>> nodeValuesMap = new HashMap<Integer, List<ValuesClause>>();
			TableConfig tableConfig = schema.getTables().get(tableName);
			AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
			for (ValuesClause valueClause : valueClauseList) {
				if (valueClause.getValues().size() != columnNum) {
					String msg = "bad insert sql columnSize != valueSize:" + columnNum + " != "
							+ valueClause.getValues().size() + "values:" + valueClause;
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}
				SQLExpr expr = valueClause.getValues().get(shardingColIndex);
				String shardingValue = null;
				if (expr instanceof SQLIntegerExpr) {
					SQLIntegerExpr intExpr = (SQLIntegerExpr) expr;
					shardingValue = intExpr.getNumber() + "";
				} else if (expr instanceof SQLCharExpr) {
					SQLCharExpr charExpr = (SQLCharExpr) expr;
					shardingValue = charExpr.getText();
				}

				Integer nodeIndex = algorithm.calculate(shardingValue);
				// 没找到插入的分片
				if (nodeIndex == null) {
					String msg = "can't find any valid datanode :" + tableName + " -> " + partitionColumn + " -> "
							+ shardingValue;
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}
				if (nodeValuesMap.get(nodeIndex) == null) {
					nodeValuesMap.put(nodeIndex, new ArrayList<ValuesClause>());
				}
				nodeValuesMap.get(nodeIndex).add(valueClause);
			}

			RouteResultsetNode[] nodes = new RouteResultsetNode[nodeValuesMap.size()];
			int count = 0;
			for (Map.Entry<Integer, List<ValuesClause>> node : nodeValuesMap.entrySet()) {
				Integer nodeIndex = node.getKey();
				List<ValuesClause> valuesList = node.getValue();
				insertStmt.setValuesList(valuesList);
				nodes[count] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex), rrs.getSqlType(),
						insertStmt.toString());
				nodes[count++].setSource(rrs);

			}
			rrs.setNodes(nodes);
			rrs.setFinishedRoute(true);
		} else if (insertStmt.getQuery() != null) {
			// insert into .... select ....
			String msg = "TODO:insert into .... select .... not supported!";
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
	}
	
	/**
	 * 寻找拆分字段在 columnList中的索引
	 *
	 * @param insertStmt
	 * @param partitionColumn
	 * @return
	 * @throws SQLNonTransientException
	 */
	private int getShardingColIndex(SchemaInfo schemaInfo, MySqlInsertStatement insertStmt, String partitionColumn) throws SQLNonTransientException {
		int shardingColIndex = -1;
		if (insertStmt.getColumns() == null || insertStmt.getColumns().size() == 0) {
			TableMeta tbMeta = MycatServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.schema, schemaInfo.table);
			if (tbMeta != null) {
				for (int i = 0; i < tbMeta.getColumnsCount(); i++) {
					if (partitionColumn.equalsIgnoreCase(tbMeta.getColumns(i).getName())) {
						return i;
					}
				}
			}
			return shardingColIndex;
		}
		for (int i = 0; i < insertStmt.getColumns().size(); i++) {
			if (partitionColumn.equalsIgnoreCase(StringUtil.removeBackquote(insertStmt.getColumns().get(i).toString()))) {
				return i;
			}
		}
		if (shardingColIndex == -1) {
			String msg = "bad insert sql, sharding column/joinKey:" + partitionColumn + " not provided," + insertStmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		return shardingColIndex;
	}

	private int getTableColumns(SchemaInfo schemaInfo, MySqlInsertStatement insertStmt)
			throws SQLNonTransientException {
		if (insertStmt.getColumns() == null || insertStmt.getColumns().size() == 0) {
			TableMeta tbMeta = MycatServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.schema, schemaInfo.table);
			if (tbMeta == null) {
				String msg = "can't find table [" + schemaInfo.table + "] define in schema:" + schemaInfo.schema;
				LOGGER.warn(msg);
				throw new SQLNonTransientException(msg);
			}
			return tbMeta.getColumnsCount();
		} else {
			return insertStmt.getColumns().size();
		}
	}

	/**
	 * 寻找joinKey的索引
	 *
	 * @param columns
	 * @param joinKey
	 * @return -1表示没找到，>=0表示找到了
	 * @throws SQLNonTransientException
	 */
	private int getJoinKeyIndex(SchemaInfo schemaInfo, MySqlInsertStatement insertStmt, String joinKey) throws SQLNonTransientException {
		return getShardingColIndex(schemaInfo, insertStmt, joinKey);
	}
	private String convertInsertSQL(SchemaInfo schemaInfo, MySqlInsertStatement insert) throws SQLNonTransientException {
		TableMeta orgTbMeta = MycatServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.schema,
				schemaInfo.table);
		if (orgTbMeta == null)
			return insert.toString();

		String tableName = schemaInfo.table;
		if (!GlobalTableUtil.isInnerColExist(schemaInfo, orgTbMeta))
			return insert.toString();


		// insert into .... select ....
		if (insert.getQuery() != null) {
			String msg = "TODO:insert into .... select .... not supported!";
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}

		StringBuilder sb = new StringBuilder(200) // 指定初始容量可以提高性能
				.append("insert into ").append(tableName);

		List<SQLExpr> columns = insert.getColumns();

		int idx = -1;
		int colSize = -1;
		// insert 没有带列名：insert into t values(xxx,xxx)
		if (columns == null || columns.size() <= 0) {
			colSize = orgTbMeta.getColumnsList().size();
			sb.append("(");
			for (int i = 0; i < colSize; i++) {
				String column = orgTbMeta.getColumnsList().get(i).getName();
				if (i > 0)
					sb.append(",");
				if (column.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN))
					idx = i; // 找到 内部列的索引位置
				sb.append(column);
			}
			sb.append(")");
		} else { // insert 语句带有 列名
			sb.append("(");
			for (int i = 0; i < columns.size(); i++) {
				if (i < columns.size() - 1)
					sb.append(columns.get(i).toString()).append(",");
				else
					sb.append(columns.get(i).toString());
				String column = StringUtil.removeBackquote(insert.getColumns().get(i).toString());
				if (column.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN))
					idx = i;
			}
			if (idx <= -1)
				sb.append(",").append(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN);
			sb.append(")");
			colSize = columns.size();
		}

		sb.append(" values");
		List<ValuesClause> vcl = insert.getValuesList();
		if (vcl != null && vcl.size() > 1) { // 批量insert
			for (int j = 0; j < vcl.size(); j++) {
				if (j != vcl.size() - 1)
					appendValues(vcl.get(j).getValues(), sb, idx, colSize).append(",");
				else
					appendValues(vcl.get(j).getValues(), sb, idx, colSize);
			}
		} else { // 非批量 insert
			List<SQLExpr> valuse = insert.getValues().getValues();
			appendValues(valuse, sb, idx, colSize);
		}

		List<SQLExpr> dku = insert.getDuplicateKeyUpdate();
		if (dku != null && dku.size() > 0) {
			boolean flag = false;
			sb.append(" on duplicate key update ");
			for (int i = 0; i < dku.size(); i++) {
				SQLExpr exp = dku.get(i);
				if(!(exp instanceof SQLBinaryOpExpr)){
					String msg = "not supported! on duplicate key update exp is "+exp.getClass();
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}
				SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr)exp;
				if(!flag && GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN.equals(binaryOpExpr.getLeft().toString())) {
					flag = true;
					onDuplicateGlobalColumn(sb);
				} else {
					sb.append(binaryOpExpr.toString());
				}
				if (i < dku.size() - 1) {
					sb.append(",");
				}
			}
			if (!flag) {
				sb.append(",");
				onDuplicateGlobalColumn(sb);
			}
		}
		return sb.toString();
	}

	private static void onDuplicateGlobalColumn(StringBuilder sb){
		sb.append(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN);
		sb.append("=values(");
		sb.append(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN);
		sb.append(")");
	}

	private static StringBuilder appendValues(List<SQLExpr> valuse, StringBuilder sb, int idx, int colSize) {
		String operationTimestamp = String.valueOf(new Date().getTime());
		int size = valuse.size();
		if (size < colSize)
			size = colSize;

		sb.append("(");
		for (int i = 0; i < size; i++) {
			if (i < size - 1) {
				if (i != idx)
					sb.append(valuse.get(i).toString()).append(",");
				else
					sb.append(operationTimestamp).append(",");
			} else {
				if (i != idx) {
					sb.append(valuse.get(i).toString());
				} else {
					sb.append(operationTimestamp);
				}
			}
		}
		if (idx <= -1)
			sb.append(",").append(operationTimestamp);
		return sb.append(")");
	}
}