package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;

import io.mycat.MycatServer;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatPrivileges;
import io.mycat.config.MycatPrivileges.Checktype;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.handler.MysqlInformationSchemaHandler;
import io.mycat.server.handler.MysqlProcHandler;
import io.mycat.server.response.InformationSchemaProfiling;
import io.mycat.server.util.SchemaUtil;
import io.mycat.server.util.SchemaUtil.SchemaInfo;
import io.mycat.sqlengine.mpp.ColumnRoutePair;

public class DruidSelectParser extends DruidBaseSelectParser {
	@Override
	public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt,
			MycatSchemaStatVisitor visitor) throws SQLNonTransientException {
		SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) selectStmt.getSelect().getQuery();
			SQLTableSource mysqlFrom = mysqlSelectQuery.getFrom();
			if (mysqlFrom == null) {
				String db = SchemaUtil.getRandomDb();
				if (db == null) {
					String msg = "No schema is configured, make sure your config is right, sql:" + stmt;
					throw new SQLNonTransientException(msg);
				}
				schema = MycatServer.getInstance().getConfig().getSchemas().get(db);
				rrs = RouterUtil.routeToMultiNode(false, rrs, schema.getMetaDataNodes(), rrs.getStatement());
				rrs.setFinishedRoute(true);
				return schema;
			}

			String schemaName = schema == null ? null : schema.getName();
			SchemaInfo schemaInfo = null;
			if (mysqlFrom instanceof SQLSubqueryTableSource || mysqlFrom instanceof SQLJoinTableSource
					|| mysqlFrom instanceof SQLUnionQueryTableSource) {
				//TODO : SQLUnionQueryTableSource USELESS
				if(MycatServer.getInstance().getConfig().getSystem().isUseExtensions()){
					rrs.setSqlStatement(stmt);
					rrs.setNeedOptimizer(true);
					rrs.setFinishedRoute(true);
					return schema;
				}
				else{
					if (schemaName == null) {
						String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
						throw new SQLNonTransientException(msg);
					}
					SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(schemaName);
					if (schemaConfig == null) {
						String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
						throw new SQLNonTransientException(msg);
					}
					schema = schemaConfig;
				}
			}
			else{
				SQLExprTableSource fromSource = (SQLExprTableSource) mysqlFrom;
				schemaInfo = SchemaUtil.getSchemaInfo(schemaName, fromSource);
				if (schemaInfo == null) {
					String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
					throw new SQLNonTransientException(msg);
				}
				// 兼容PhpAdmin's, 支持对MySQL元数据的模拟返回
				if (SchemaUtil.INFORMATION_SCHEMA.equalsIgnoreCase(schemaInfo.schema)) {
					MysqlInformationSchemaHandler.handle(schemaInfo, rrs.getSession().getSource());
					rrs.setFinishedExecute(true);
					return schema;
				}
	
				if (SchemaUtil.MYSQL_SCHEMA.equalsIgnoreCase(schemaInfo.schema)
						&& SchemaUtil.TABLE_PROC.equalsIgnoreCase(schemaInfo.table)) {
					// 兼容MySQLWorkbench
					MysqlProcHandler.handle(rrs.getStatement(), rrs.getSession().getSource());
					rrs.setFinishedExecute(true);
					return schema;
				}
				// fix navicat SELECT STATE AS `State`, ROUND(SUM(DURATION),7) AS
				// `Duration`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS
				// `Percentage` FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID=
				// GROUP BY STATE ORDER BY SEQ
				if (SchemaUtil.INFORMATION_SCHEMA.equalsIgnoreCase(schemaInfo.schema)
						&& SchemaUtil.TABLE_PROFILING.equalsIgnoreCase(schemaInfo.table)
						&& rrs.getStatement().toUpperCase().contains("CONCAT(ROUND(SUM(DURATION)/*100,3)")) {
					InformationSchemaProfiling.response(rrs.getSession().getSource());
					rrs.setFinishedExecute(true);
					return schema;
				}
				if (schemaInfo.schemaConfig == null) {
					String msg = "No MyCAT Database is selected Or defined, sql:" + stmt;
					throw new SQLNonTransientException(msg);
				}
				if (!MycatPrivileges.checkPrivilege(rrs, schemaInfo.schema, schemaInfo.table, Checktype.SELECT)) {
					String msg = "The statement DML privilege check is not passed, sql:" + stmt;
					throw new SQLNonTransientException(msg);
				}
				if (fromSource.getExpr() instanceof SQLPropertyExpr) {
					fromSource.setExpr(new SQLIdentifierExpr(schemaInfo.table));
					String sqlWithoutSchema = stmt.toString();
					ctx.setSql(sqlWithoutSchema);
				}
				rrs.setStatement(ctx.getSql());
				schema = schemaInfo.schemaConfig;
			}
			super.visitorParse(schema, rrs, stmt, visitor);
			parseOrderAggGroupMysql(schema, stmt, rrs, mysqlSelectQuery);
			// 更改canRunInReadDB属性
			if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode())
					&& rrs.isAutocommit() == false) {
				rrs.setCanRunInReadDB(false);
			}
		} else if (sqlSelectQuery instanceof MySqlUnionQuery && MycatServer.getInstance().getConfig().getSystem().isUseExtensions()) {
			rrs.setSqlStatement(stmt);
			rrs.setNeedOptimizer(true);
			rrs.setFinishedRoute(true);
		}
		return schema;
	}
	/**
	 * 改写sql：需要加limit的加上
	 */
	@Override
	public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, LayerCachePool cachePool)
			throws SQLNonTransientException {
		if (rrs.isFinishedExecute() || rrs.isNeedOptimizer()) {
			return;
		}
		tryRoute(schema, rrs, cachePool);
		rrs.copyLimitToNodes();
		SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
		SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
		if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) selectStmt.getSelect().getQuery();
			int limitStart = 0;
			int limitSize = schema.getDefaultMaxLimit();

			// clear group having
			SQLSelectGroupByClause groupByClause = mysqlSelectQuery.getGroupBy();
			// Modified by winbill, 20160614, do NOT include having clause when
			// routing to multiple nodes
			if (groupByClause != null && groupByClause.getHaving() != null && isRoutMultiNode(schema, rrs)) {
				groupByClause.setHaving(null);
			}

			Map<String, Map<String, Set<ColumnRoutePair>>> allConditions = getAllConditions();
			boolean isNeedAddLimit = isNeedAddLimit(schema, rrs, mysqlSelectQuery, allConditions);
			if (isNeedAddLimit) {
				Limit limit = new Limit();
				limit.setRowCount(new SQLIntegerExpr(limitSize));
				mysqlSelectQuery.setLimit(limit);
				rrs.setLimitSize(limitSize);
				String sql = getSql(rrs, stmt, isNeedAddLimit);
				rrs.changeNodeSqlAfterAddLimit(schema, sql, 0, limitSize);

			}
			Limit limit = mysqlSelectQuery.getLimit();
			if (limit != null && !isNeedAddLimit) {
				SQLIntegerExpr offset = (SQLIntegerExpr) limit.getOffset();
				SQLIntegerExpr count = (SQLIntegerExpr) limit.getRowCount();
				if (offset != null) {
					limitStart = offset.getNumber().intValue();
					rrs.setLimitStart(limitStart);
				}
				if (count != null) {
					limitSize = count.getNumber().intValue();
					rrs.setLimitSize(limitSize);
				}

				if (isNeedChangeLimit(rrs)) {
					Limit changedLimit = new Limit();
					changedLimit.setRowCount(new SQLIntegerExpr(limitStart + limitSize));

					if (offset != null) {
						if (limitStart < 0) {
							String msg = "You have an error in your SQL syntax; check the manual that "
									+ "corresponds to your MySQL server version for the right syntax to use near '"
									+ limitStart + "'";
							throw new SQLNonTransientException(ErrorCode.ER_PARSE_ERROR + " - " + msg);
						} else {
							changedLimit.setOffset(new SQLIntegerExpr(0));

						}
					}

					mysqlSelectQuery.setLimit(changedLimit);
					String sql = getSql(rrs, stmt, isNeedAddLimit);
					rrs.changeNodeSqlAfterAddLimit(schema, sql, 0, limitStart + limitSize);

					// 设置改写后的sql
					ctx.setSql(sql);
				} else {
					rrs.changeNodeSqlAfterAddLimit(schema, getCtx().getSql(), rrs.getLimitStart(), rrs.getLimitSize());
					// ctx.setSql(nativeSql);
				}

			}

			rrs.setCacheAble(isNeedCache(schema, rrs, mysqlSelectQuery, allConditions));
		}

	}
	
	/**
	 * 获取所有的条件：因为可能被or语句拆分成多个RouteCalculateUnit，条件分散了
	 * @return
	 */
	private Map<String, Map<String, Set<ColumnRoutePair>>> getAllConditions() {
		Map<String, Map<String, Set<ColumnRoutePair>>> map = new HashMap<String, Map<String, Set<ColumnRoutePair>>>();
		for(RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
			if(unit != null && unit.getTablesAndConditions() != null) {
				map.putAll(unit.getTablesAndConditions());
			}
		}
		
		return map;
	}


	protected String getSql(RouteResultset rrs, SQLStatement stmt, boolean isNeedAddLimit) {
		if ((isNeedChangeLimit(rrs) || isNeedAddLimit)) {
			return stmt.toString();
		}
		return getCtx().getSql();
	}


	
	protected boolean isNeedChangeLimit(RouteResultset rrs) {
		if(rrs.getNodes() == null) {
			return false;
		} else {
			if(rrs.getNodes().length > 1) {
				return true;
			}
			return false;
		
		} 
	}
	
	private boolean isNeedCache(SchemaConfig schema, RouteResultset rrs, 
			MySqlSelectQueryBlock mysqlSelectQuery, Map<String, Map<String, Set<ColumnRoutePair>>> allConditions) {
		if(ctx.getTables() == null || ctx.getTables().size() == 0 ) {
			return false;
		}
		TableConfig tc = schema.getTables().get(ctx.getTables().get(0));
		if(tc==null ||(ctx.getTables().size() == 1 && tc.isGlobalTable())
				) {//|| (ctx.getTables().size() == 1) && tc.getRule() == null && tc.getDataNodes().size() == 1
			return false;
		} else {
			//单表主键查询
			if(ctx.getTables().size() == 1) {
				String tableName = ctx.getTables().get(0);
				String primaryKey = schema.getTables().get(tableName).getPrimaryKey();
//				schema.getTables().get(ctx.getTables().get(0)).getParentKey() != null;
				if(ctx.getRouteCalculateUnit().getTablesAndConditions().get(tableName) != null
						&& ctx.getRouteCalculateUnit().getTablesAndConditions().get(tableName).get(primaryKey) != null 
						&& tc.getDataNodes().size() > 1) {//有主键条件
					return false;
				} 
			}
			return true;
		}
	}
	
	/**
	 * 单表且是全局表
	 * 单表且rule为空且nodeNodes只有一个
	 * @param schema
	 * @param rrs
	 * @param mysqlSelectQuery
	 * @return
	 */
	private boolean isNeedAddLimit(SchemaConfig schema, RouteResultset rrs, 
			MySqlSelectQueryBlock mysqlSelectQuery, Map<String, Map<String, Set<ColumnRoutePair>>> allConditions) {
//		ctx.getTablesAndConditions().get(key))
		  if(rrs.getLimitSize()>-1)
		  {
			  return false;
		  }else
		if(schema.getDefaultMaxLimit() == -1) {
			return false;
		} else if (mysqlSelectQuery.getLimit() != null) {//语句中已有limit
			return false;
		} else if(ctx.getTables().size() == 1) {
			String tableName = ctx.getTables().get(0);
			TableConfig tableConfig = schema.getTables().get(tableName);
			if(tableConfig==null)
			{
			 return    schema.getDefaultMaxLimit() > -1;   //   找不到则取schema的配置
			}

			boolean isNeedAddLimit= tableConfig.isNeedAddLimit();
			if(!isNeedAddLimit)
			{
				return false;//优先从配置文件取
			}

			if(schema.getTables().get(tableName).isGlobalTable()) {
				return true;
			}

			String primaryKey = schema.getTables().get(tableName).getPrimaryKey();

//			schema.getTables().get(ctx.getTables().get(0)).getParentKey() != null;
			if(allConditions.get(tableName) == null) {//无条件
				return true;
			}
			
			if (allConditions.get(tableName).get(primaryKey) != null) {//条件中带主键
				return false;
			}
			
			return true;
		} else if(rrs.hasPrimaryKeyToCache() && ctx.getTables().size() == 1){//只有一个表且条件中有主键,不需要limit了,因为主键只能查到一条记录
			return false;
		} else {//多表或无表
			return false;
		}
		
	}
	
	}
