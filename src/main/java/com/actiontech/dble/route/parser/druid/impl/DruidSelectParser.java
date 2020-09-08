/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.GlobalTableConfig;
import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.config.privileges.ShardingPrivileges;
import com.actiontech.dble.config.privileges.ShardingPrivileges.CheckType;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemCreate;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.plan.visitor.MySQLItemVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.handler.MysqlSystemSchemaHandler;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.sqlengine.mpp.ColumnRoute;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

public class DruidSelectParser extends DefaultDruidParser {
    private static HashSet<String> aggregateSet = new HashSet<>(16, 1);

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
                                     ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain) throws SQLException {
        SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
        String schemaName = schema == null ? null : schema.getName();
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            //check the select into sql is not supported
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) sqlSelectQuery;
            if (mysqlSelectQuery.getInto() != null) {
                throw new SQLNonTransientException("select ... into is not supported!");
            }

            //three types of select route according to the from item in select sql
            SQLTableSource mysqlFrom = mysqlSelectQuery.getFrom();
            if (mysqlFrom == null) {
                routeForNoFrom(schema, rrs, visitor, isExplain, service, selectStmt);
                return schema;
            } else if (mysqlFrom instanceof SQLExprTableSource) {
                SQLExprTableSource fromSource = (SQLExprTableSource) mysqlFrom;
                SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, fromSource);
                if (schemaInfo.isDual()) {
                    //dual just route for a Random shardingNode
                    RouterUtil.routeNoNameTableToSingleNode(rrs, schema);
                    return schema;
                } else if (SchemaUtil.MYSQL_SYS_SCHEMA.contains(schemaInfo.getSchema().toUpperCase())) {
                    //sys_schema just use a special handler to response
                    MysqlSystemSchemaHandler.handle(service, schemaInfo, mysqlSelectQuery);
                    rrs.setFinishedExecute(true);
                    return schema;
                } else {
                    //normal sharding in config
                    if (!ShardingPrivileges.checkPrivilege(service.getUserConfig(), schemaInfo.getSchema(), schemaInfo.getTable(), CheckType.SELECT)) {
                        String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
                        throw new SQLNonTransientException(msg);
                    }
                    super.visitorParse(schema != null ? schema : schemaInfo.getSchemaConfig(), rrs, stmt, visitor, service, isExplain);
                    //check to route for complex
                    if (ProxyMeta.getInstance().getTmManager().getSyncView(schemaInfo.getSchemaConfig().getName(), schemaInfo.getTable()) != null ||
                            hasInnerFuncSelect(visitor.getFunctions())) {
                        rrs.setNeedOptimizer(true);
                        rrs.setSqlStatement(selectStmt);
                        return schemaInfo.getSchemaConfig();
                    }
                    if (visitor.getSubQueryList().size() > 0) {
                        return executeComplexSQL(schemaName, schema, rrs, selectStmt, service, visitor.getSelectTableList().size(), visitor.isContainsInnerFunction());
                    }

                    //route for single table
                    routeSingleTable(rrs, schemaInfo, mysqlSelectQuery, selectStmt, service);
                    return schema;
                }

            } else if (mysqlFrom instanceof SQLSubqueryTableSource ||
                    mysqlFrom instanceof SQLJoinTableSource ||
                    mysqlFrom instanceof SQLUnionQueryTableSource) {
                super.visitorParse(schema, rrs, stmt, visitor, service, isExplain);
                return executeComplexSQL(schemaName, schema, rrs, selectStmt, service, visitor.getSelectTableList().size(), visitor.isContainsInnerFunction());
            }
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            super.visitorParse(schema, rrs, stmt, visitor, service, isExplain);
            return executeComplexSQL(schemaName, schema, rrs, selectStmt, service, visitor.getSelectTableList().size(), visitor.isContainsInnerFunction());
        }
        return schema;
    }


    private void routeSingleTable(RouteResultset rrs, SchemaInfo schemaInfo, MySqlSelectQueryBlock mysqlSelectQuery,
                                  SQLSelectStatement selectStmt, ShardingService service) throws SQLException {
        rrs.setSchema(schemaInfo.getSchema());
        rrs.setTable(schemaInfo.getTable());
        rrs.setTableAlias(schemaInfo.getTableAlias());
        rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
        SchemaConfig schema = schemaInfo.getSchemaConfig();

        String noShardingNode = RouterUtil.isNoSharding(schema, schemaInfo.getTable());
        if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && !service.isAutocommit()) {
            rrs.setCanRunInReadDB(false);
        }
        if (noShardingNode != null) {
            //route to singleNode
            RouterUtil.routeToSingleNode(rrs, noShardingNode);
        } else {
            //route for configured table
            BaseTableConfig tc = schema.getTables().get(schemaInfo.getTable());
            if (tc == null) {
                String msg = "Table '" + schema.getName() + "." + schemaInfo.getTable() + "' doesn't exist";
                throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
            }

            //loop conditions to determine the scope
            SortedSet<RouteResultsetNode> nodeSet = new TreeSet<>();
            for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
                if (unit.isAlwaysFalse()) {
                    rrs.setAlwaysFalse(true);
                }
                RouteResultset rrsTmp = RouterUtil.tryRouteForOneTable(schema, unit, tc.getName(), rrs, true, service.getCharset().getClient());
                if (rrsTmp != null && rrsTmp.getNodes() != null) {
                    Collections.addAll(nodeSet, rrsTmp.getNodes());
                    if (rrsTmp.isGlobalTable()) {
                        break;
                    }
                }
            }

            if (nodeSet.size() == 0) {
                String msg = " find no Route:" + rrs.getStatement();
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            } else if (nodeSet.size() > 1) {
                //if the sql involved node more than 1 ,Aggregate function/Group by/Order by should use complexQuery
                parseOrderAggGroupMysql(schema, selectStmt, rrs, mysqlSelectQuery, tc);
                if (rrs.isNeedOptimizer()) {
                    rrs.setNodes(null);
                    return;
                }
            }
            RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
            int i = 0;
            for (RouteResultsetNode aNodeSet : nodeSet) {
                nodes[i] = aNodeSet;
                i++;
            }
            rrs.setNodes(nodes);
            rrs.setFinishedRoute(true);
        }
    }

    /**
     * route for from is null
     * check the sql subquery first
     * route for a NoNameTable
     */
    private void routeForNoFrom(SchemaConfig schema, RouteResultset rrs, ServerSchemaStatVisitor visitor, boolean isExplain, ShardingService service,
                                SQLSelectStatement selectStmt) throws SQLException {
        super.visitorParse(schema, rrs, selectStmt, visitor, service, isExplain);
        if (visitor.getSubQueryList().size() > 0) {
            executeComplexSQL(schema.getName(), schema, rrs, selectStmt, service, visitor.getSelectTableList().size(), visitor.isContainsInnerFunction());
            return;
        }
        RouterUtil.routeNoNameTableToSingleNode(rrs, schema);
    }


    private boolean hasInnerFuncSelect(List<SQLMethodInvokeExpr> funcList) {
        if (funcList != null) {
            for (SQLMethodInvokeExpr expr : funcList) {
                if (ItemCreate.getInstance().isInnerFunc(expr.getMethodName())) {
                    return true;
                }
            }
        }
        return false;
    }

    private void tryRouteToOneNodeForComplex(RouteResultset rrs, SQLSelectStatement selectStmt, int tableSize, String clientCharset) throws SQLException {
        Set<String> schemaList = new HashSet<>();
        String shardingNode = RouterUtil.tryRouteTablesToOneNodeForComplex(rrs, ctx, schemaList, tableSize, clientCharset);
        if (shardingNode != null) {
            String sql = rrs.getStatement();
            for (String toRemoveSchemaName : schemaList) {
                sql = RouterUtil.removeSchema(sql, toRemoveSchemaName);
            }
            rrs.setStatement(sql);
            RouterUtil.routeToSingleNode(rrs, shardingNode);
        } else {
            rrs.setNeedOptimizer(true);
            rrs.setSqlStatement(selectStmt);
        }
    }


    private void parseOrderAggGroupMysql(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs,
                                         MySqlSelectQueryBlock mysqlSelectQuery, BaseTableConfig tc) throws SQLException {
        //simple merge of ORDER BY has bugs,so optimizer here
        if (mysqlSelectQuery.getOrderBy() != null) {
            tryAddLimit(tc, mysqlSelectQuery);
            rrs.setSqlStatement(stmt);
            rrs.setNeedOptimizer(true);
            return;
        }
        parseAggGroupCommon(schema, stmt, rrs, mysqlSelectQuery, tc);
    }

    private void parseAggExprCommon(SchemaConfig schema, RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery, Map<String, String> aliaColumns, BaseTableConfig tc, boolean isDistinct) throws SQLException {
        List<SQLSelectItem> selectList = mysqlSelectQuery.getSelectList();
        boolean hasPartitionColumn = false;
        for (SQLSelectItem selectItem : selectList) {
            SQLExpr itemExpr = selectItem.getExpr();
            if (itemExpr instanceof SQLQueryExpr) { // can not be reach
                rrs.setNeedOptimizer(true);
                return;
            } else if (itemExpr instanceof SQLAggregateExpr) {
                /*
                 * MAX,MIN; SUM,COUNT without distinct is not need optimize, but
                 * there is bugs in default Aggregate IN FACT ,ONLY:
                 * SUM(distinct ),COUNT(distinct),AVG,STDDEV,GROUP_CONCAT
                 */
                rrs.setNeedOptimizer(true);
                return;
            } else if (itemExpr instanceof SQLMethodInvokeExpr) {
                String methodName = ((SQLMethodInvokeExpr) itemExpr).getMethodName().toUpperCase();
                if (aggregateSet.contains(methodName)) {
                    rrs.setNeedOptimizer(true);
                    return;
                } else if (isSumFuncOrSubQuery(schema.getName(), itemExpr)) {
                    rrs.setNeedOptimizer(true);
                    return;
                } else {
                    addToAliaColumn(aliaColumns, selectItem);
                }
            } else if (itemExpr instanceof SQLAllColumnExpr) {
                TableMeta tbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schema.getName(), tc.getName());
                if (tbMeta == null) {
                    String msg = "Meta data of table '" + schema.getName() + "." + tc.getName() + "' doesn't exist";
                    LOGGER.info(msg);
                    throw new SQLNonTransientException(msg);
                }
                for (ColumnMeta column : tbMeta.getColumns()) {
                    aliaColumns.put(column.getName(), column.getName());
                }
            } else {
                if (isDistinct && !isNeedOptimizer(itemExpr)) {
                    if (itemExpr instanceof SQLIdentifierExpr) {
                        SQLIdentifierExpr item = (SQLIdentifierExpr) itemExpr;
                        if (hasShardingColumn(tc, item.getSimpleName())) hasPartitionColumn = true;
                        addToAliaColumn(aliaColumns, selectItem);
                    } else if (itemExpr instanceof SQLPropertyExpr) {
                        SQLPropertyExpr item = (SQLPropertyExpr) itemExpr;
                        if (hasShardingColumn(tc, item.getSimpleName())) hasPartitionColumn = true;
                        addToAliaColumn(aliaColumns, selectItem);
                    }
                } else if (isSumFuncOrSubQuery(schema.getName(), itemExpr)) {
                    rrs.setNeedOptimizer(true);
                    return;
                } else {
                    addToAliaColumn(aliaColumns, selectItem);
                }
            }
        }
        if (isDistinct && !hasPartitionColumn) {
            rrs.setNeedOptimizer(true);
            return;
        }
        parseGroupCommon(rrs, mysqlSelectQuery, tc);
    }

    private void parseGroupCommon(RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery, BaseTableConfig tc) {
        if (mysqlSelectQuery.getGroupBy() != null) {
            SQLSelectGroupByClause groupBy = mysqlSelectQuery.getGroupBy();
            boolean hasPartitionColumn = false;
            for (SQLExpr groupByItem : groupBy.getItems()) {
                if (isNeedOptimizer(groupByItem)) {
                    rrs.setNeedOptimizer(true);
                    return;
                } else if (groupByItem instanceof SQLIdentifierExpr) {
                    SQLIdentifierExpr item = (SQLIdentifierExpr) groupByItem;
                    hasPartitionColumn = hasShardingColumn(tc, item.getSimpleName());
                } else if (groupByItem instanceof SQLPropertyExpr) {
                    SQLPropertyExpr item = (SQLPropertyExpr) groupByItem;
                    hasPartitionColumn = hasShardingColumn(tc, item.getSimpleName());
                }
            }
            if (groupBy.getItems().size() > 0 && !hasPartitionColumn) {
                rrs.setNeedOptimizer(true);
                return;
            }
            if (groupBy.getItems().size() == 0 && groupBy.getHaving() != null) {
                // only having filter need optimizer
                rrs.setNeedOptimizer(true);
            }
        }
    }

    private boolean hasShardingColumn(BaseTableConfig tc, String columnName) {
        return tc instanceof ShardingTableConfig && columnName.equalsIgnoreCase(((ShardingTableConfig) tc).getShardingColumn());
    }

    private boolean isSumFuncOrSubQuery(String schema, SQLExpr itemExpr) {
        MySQLItemVisitor ev = new MySQLItemVisitor(schema, CharsetUtil.getCharsetDefaultIndex("utf8mb4"), ProxyMeta.getInstance().getTmManager(), null);
        itemExpr.accept(ev);
        Item selItem = ev.getItem();
        return contaisSumFuncOrSubquery(selItem);
    }

    private boolean contaisSumFuncOrSubquery(Item selItem) {
        if (selItem.isWithSumFunc()) {
            return true;
        }
        if (selItem.isWithSubQuery()) {
            return true;
        }
        if (selItem.getArgCount() > 0) {
            for (Item child : selItem.arguments()) {
                if (contaisSumFuncOrSubquery(child)) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }

    private boolean isNeedOptimizer(SQLExpr expr) {
        // it is NotSimpleColumn TODO: check every expr to decide it is NeedOptimizer
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

    private String getFieldName(SQLSelectItem item) {
        if ((item.getExpr() instanceof SQLPropertyExpr) || (item.getExpr() instanceof SQLMethodInvokeExpr) ||
                (item.getExpr() instanceof SQLIdentifierExpr) || item.getExpr() instanceof SQLBinaryOpExpr) {
            return item.getExpr().toString(); // alias
        } else {
            return item.toString();
        }
    }

    private void parseAggGroupCommon(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs,
                                     MySqlSelectQueryBlock mysqlSelectQuery, BaseTableConfig tc) throws SQLException {
        Map<String, String> aliaColumns = new HashMap<>();
        boolean isDistinct = (mysqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCT) || (mysqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCTROW);
        parseAggExprCommon(schema, rrs, mysqlSelectQuery, aliaColumns, tc, isDistinct);
        if (rrs.isNeedOptimizer()) {
            tryAddLimit(tc, mysqlSelectQuery);
            rrs.setSqlStatement(stmt);
            return;
        }

        // distinct change to group by
        if (isDistinct) {
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
        }

        if (isDistinct) {
            rrs.changeNodeSqlAfterAddLimit(statementToString(stmt), 0, -1);
        }
    }

    private String getAliaColumn(Map<String, String> aliaColumns, String column) {
        String alia = aliaColumns.get(column);
        if (alia == null) {
            if (!column.contains(".")) {
                String col = "." + column;
                String col2 = ".`" + column + "`";
                // for aliaColumns,change <c.name,cname> to <c.name,cname> and <name,cname>
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
                column = sqlExpr.toString();
            } else if (sqlExpr instanceof MySqlOrderingExpr) {
                // todo czn
                SQLExpr expr = ((MySqlOrderingExpr) sqlExpr).getExpr();

                if (expr instanceof SQLName) {
                    column = StringUtil.removeBackQuote(((SQLName) expr).getSimpleName());
                } else {
                    column = StringUtil.removeBackQuote(expr.toString());
                }
            } else if (sqlExpr instanceof SQLPropertyExpr) {
                /*
                 * eg:select id from (select h.id from hotnews h union
                 * select h.title from hotnews h ) as t1 group by t1.id;
                 */
                column = sqlExpr.toString();
            }
            if (column == null) {
                column = sqlExpr.toString();
            }
            int dotIndex = column.indexOf(".");
            int bracketIndex = column.indexOf("(");
            // check it is a function
            if (dotIndex != -1 && bracketIndex == -1) {
                // get column from table.column
                column = column.substring(dotIndex + 1);
            }
            groupByCols[i] = getAliaColumn(aliaColumns, column); // column;
        }
        return groupByCols;
    }

    private SchemaConfig executeComplexSQL(
            String schemaName, SchemaConfig schema, RouteResultset rrs,
            SQLSelectStatement selectStmt, ShardingService service, int tableSize, boolean containsInnerFunction)
            throws SQLException {
        StringPtr noShardingNode = new StringPtr(null);
        Set<String> schemas = new HashSet<>();
        if (SchemaUtil.isNoSharding(service, selectStmt.getSelect().getQuery(), selectStmt, selectStmt, schemaName, schemas, noShardingNode)) {
            return routeToNoSharding(schema, rrs, schemas, noShardingNode);
        } else if (schemas.size() > 0 && SchemaUtil.MYSQL_SYS_SCHEMA.containsAll(schemas)) {
            MysqlSystemSchemaHandler.handle(service, null, selectStmt.getSelect().getQuery());
            rrs.setFinishedExecute(true);
            return schema;
        } else if (containsInnerFunction) {
            rrs.setNeedOptimizer(true);
            rrs.setSqlStatement(selectStmt);
            return schema;
        } else {
            tryRouteToOneNodeForComplex(rrs, selectStmt, tableSize, service.getCharset().getClient());
            return schema;
        }
    }

    /**
     * changeSql: add limit if need
     */
    @Override
    public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
            throws SQLException {
        if (rrs.isFinishedExecute() || rrs.isNeedOptimizer()) {
            return;
        }
        rrs.copyLimitToNodes();
        SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
        SchemaConfig sqlSchema = DbleServer.getInstance().getConfig().getSchemas().get(rrs.getSchema());
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock && sqlSchema != null) {
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) selectStmt.getSelect().getQuery();
            int limitStart = 0;
            int limitSize = sqlSchema.getDefaultMaxLimit();

            Map<Pair<String, String>, Map<String, ColumnRoute>> allConditions = getAllConditions();
            int needAddLimitSize = needAddLimitSize(sqlSchema, rrs, mysqlSelectQuery, allConditions);
            if (needAddLimitSize >= 0) {
                SQLLimit limit = new SQLLimit();
                limit.setRowCount(new SQLIntegerExpr(limitSize));
                mysqlSelectQuery.setLimit(limit);
                rrs.setLimitSize(limitSize);
                String sql = RouterUtil.removeSchema(statementToString(stmt), sqlSchema.getName());
                rrs.changeNodeSqlAfterAddLimit(sql, 0, limitSize);
            }
            SQLLimit limit = mysqlSelectQuery.getLimit();
            if (limit != null && needAddLimitSize < 0) {
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
                    SQLLimit changedLimit = new SQLLimit();
                    changedLimit.setRowCount(new SQLIntegerExpr(limitStart + limitSize));

                    if (offset != null) {
                        if (limitStart < 0) {
                            String msg = "You have an error in your SQL syntax; check the manual that " +
                                    "corresponds to your MySQL server version for the right syntax to use near '" +
                                    limitStart + "'";
                            throw new SQLNonTransientException(ErrorCode.ER_PARSE_ERROR + " - " + msg);
                        } else {
                            changedLimit.setOffset(new SQLIntegerExpr(0));

                        }
                    }

                    mysqlSelectQuery.setLimit(changedLimit);
                    String sql = RouterUtil.removeSchema(statementToString(stmt), sqlSchema.getName());
                    rrs.changeNodeSqlAfterAddLimit(sql, 0, limitStart + limitSize);
                } else {
                    rrs.changeNodeSqlAfterAddLimit(rrs.getStatement(), rrs.getLimitStart(), rrs.getLimitSize());
                }
            }
            rrs.setSqlRouteCacheAble(isNeedSqlRouteCache(sqlSchema));
        }

    }

    /**
     * getAllConditions
     */
    private Map<Pair<String, String>, Map<String, ColumnRoute>> getAllConditions() {
        Map<Pair<String, String>, Map<String, ColumnRoute>> map = new HashMap<>();
        for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
            if (unit != null && unit.getTablesAndConditions() != null) {
                map.putAll(unit.getTablesAndConditions());
            }
        }
        return map;
    }


    private boolean isNeedChangeLimit(RouteResultset rrs) {
        return rrs.getNodes() != null && rrs.getNodes().length > 1;
    }

    private boolean isNeedSqlRouteCache(SchemaConfig schema) {
        if (ctx.getTables() == null || ctx.getTables().size() == 0) {
            return false;
        }
        Pair<String, String> table = ctx.getTables().get(0);
        String tableName = table.getValue();
        BaseTableConfig tc = schema.getTables().get(tableName);
        if (tc == null || (ctx.getTables().size() == 1 && tc instanceof GlobalTableConfig)) {
            return false;
        } else {
            //single table
            if (ctx.getTables().size() == 1) {
                for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
                    if (unit.getTablesAndConditions().get(table) != null &&
                            tc.getShardingNodes().size() > 1) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private void tryAddLimit(BaseTableConfig tableConfig,
                             MySqlSelectQueryBlock mysqlSelectQuery) {
        if (mysqlSelectQuery.getLimit() != null) {
            return;
        } else if (tableConfig.getMaxLimit() == -1) {
            return;
        } else if (mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) {
            return;
        }
        SQLLimit limit = new SQLLimit();
        limit.setRowCount(new SQLIntegerExpr(tableConfig.getMaxLimit()));
        mysqlSelectQuery.setLimit(limit);
    }

    private int needAddLimitSize(SchemaConfig schema, RouteResultset rrs,
                                 MySqlSelectQueryBlock mysqlSelectQuery, Map<Pair<String, String>, Map<String, ColumnRoute>> allConditions) {
        if (rrs.getLimitSize() > -1) {
            return -1;
        } else if (mysqlSelectQuery.getLimit() != null) { // has already limit
            return -1;
        } else if (ctx.getTables().size() == 1) {
            Pair<String, String> table = ctx.getTables().get(0);
            String tableName = table.getValue();
            BaseTableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig == null) {
                return schema.getDefaultMaxLimit(); // get sharding's configure
            }
            // no condition
            return allConditions.get(table) == null ? tableConfig.getMaxLimit() : -1;
        } else { // no table or multi-table
            return -1;
        }

    }

}
