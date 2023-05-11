/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.GlobalTableConfig;
import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.config.privileges.ShardingPrivileges;
import com.actiontech.dble.config.privileges.ShardingPrivileges.CheckType;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
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
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;
import java.util.stream.Collectors;

public class DruidSelectParser extends DefaultDruidParser {
    private static final Logger LOGGER = LogManager.getLogger(DruidSelectParser.class);
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
                if (!CollectionUtil.isEmpty(ctx.getTables())) {
                    service.getSession2().trace(t -> t.addTable(ctx.getTables()));
                }
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
        if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode() || mysqlSelectQuery.isForShare())) {
            if (!service.isAutocommit()) {
                rrs.setCanRunInReadDB(false);
            } else {
                rrs.setForUpdate(true);
            }
        }
        if (noShardingNode != null) {
            //route to singleNode
            RouterUtil.routeToSingleNode(rrs, noShardingNode, Sets.newHashSet(schemaInfo.getSchema() + "." + schemaInfo.getTable()));
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
                if (rrs.isRoutePenetration()) {
                    LOGGER.debug("the query {} match the route penetration regex", rrs.getSrcStatement());
                    rrs = tryDirectRoute(schema, rrs);
                    if (rrs.isFinishedRoute()) {
                        LOGGER.debug("the query {} match the route penetration rule, will direct route", rrs.getSrcStatement());
                        return;
                    }
                }
                //if the sql involved node more than 1 ,Aggregate function/Group by/Order by should use complexQuery
                parseOrderAggGroupMysql(service, schema, selectStmt, rrs, mysqlSelectQuery, tc);
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

    private RouteResultset tryDirectRoute(SchemaConfig schemaConfig, RouteResultset rrs) throws SQLException {
        if (schemaConfig == null) {
            return rrs;
        }

        List<Pair<String, String>> tables = ctx.getTables();
        Set<String> intersectionNodeSet = new HashSet<>();
        Map<String, BaseTableConfig> tableConfigMap = schemaConfig.getTables() == null ? null : schemaConfig.getTables();
        if (tableConfigMap != null) {
            for (Pair<String, String> table : tables) {
                String tableName = table.getValue();
                BaseTableConfig tc = tableConfigMap.get(tableName);
                if (tc == null) {
                    Map<String, String> tableAliasMap = ctx.getTableAliasMap();
                    if (tableAliasMap != null && tableAliasMap.get(tableName) != null) {
                        tc = tableConfigMap.get(tableAliasMap.get(tableName));
                    }
                }

                //intersection of shardnodes
                if (tc != null) {
                    if (intersectionNodeSet.isEmpty()) {
                        intersectionNodeSet.addAll(tc.getShardingNodes());
                    } else {
                        List<String> shardingNodes = tc.getShardingNodes();
                        Set<String> otherDataNode = Sets.newHashSet(shardingNodes);
                        intersectionNodeSet = Sets.intersection(intersectionNodeSet, otherDataNode);
                    }
                }
            }
        }

        RouteResultset rrsResult = rrs;
        if (!intersectionNodeSet.isEmpty()) {
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaConfig.getName()));
            rrsResult = tryRoute(schemaConfig, rrs);
        }
        return rrsResult;
    }

    private RouteResultset tryRoute(SchemaConfig schema, RouteResultset rrs) throws SQLException {
        if ((ctx.getTables() == null || ctx.getTables().size() == 0) && (ctx.getTableAliasMap() == null || ctx.getTableAliasMap().isEmpty())) {
            rrs = RouterUtil.routeToSingleNode(rrs, schema.getRandomShardingNode(), null);
            rrs.setSchema(schema.getName());
            rrs.setFinishedRoute(true);
            return rrs;
        }
        SortedSet<RouteResultsetNode> nodeSet = new TreeSet<>();
        boolean isAllGlobalTable = RouterUtil.isAllGlobalTable(ctx, schema);
        for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
            RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, ctx, unit, rrs, true, null);
            if (rrsTmp != null && rrsTmp.getNodes() != null) {
                nodeSet.addAll(Arrays.asList(rrsTmp.getNodes()));
            }
            if (isAllGlobalTable) {
                break;
            }
        }

        if (nodeSet.size() > 0) {
            RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
            int i = 0;
            for (RouteResultsetNode routeResultsetNode : nodeSet) {
                nodes[i++] = routeResultsetNode;
            }

            rrs.setNodes(nodes);
            rrs.setSchema(schema.getName());
            rrs.setFinishedRoute(true);

        }
        return rrs;
    }


    private void tryRouteToOneNodeForComplex(RouteResultset rrs, SQLSelectStatement selectStmt, int tableSize, String clientCharset) throws SQLException {
        Set<String> schemaList = new HashSet<>();
        String shardingNode = RouterUtil.tryRouteTablesToOneNodeForComplex(rrs, ctx, schemaList, tableSize, clientCharset, selectStmt);
        String sql = rrs.getStatement();
        for (String toRemoveSchemaName : schemaList) {
            sql = RouterUtil.removeSchema(sql, toRemoveSchemaName);
        }
        rrs.setStatement(sql);
        if (shardingNode != null) {
            Set<String> tableSet = ctx.getTables().stream().map(tableEntry -> tableEntry.getKey() + "." + tableEntry.getValue()).collect(Collectors.toSet());
            RouterUtil.routeToSingleNode(rrs, shardingNode, tableSet);
        } else {
            rrs.setNeedOptimizer(true);
            rrs.setSqlStatement(selectStmt);
        }
    }


    private void parseOrderAggGroupMysql(ShardingService service, SchemaConfig schema, SQLStatement stmt, RouteResultset rrs,
                                         MySqlSelectQueryBlock mysqlSelectQuery, BaseTableConfig tc) throws SQLException {
        //simple merge of ORDER BY has bugs,so optimizer here
        if (mysqlSelectQuery.getOrderBy() != null) {
            tryAddLimit(tc, mysqlSelectQuery);
            rrs.setSqlStatement(stmt);
            rrs.setNeedOptimizer(true);
            return;
        }
        parseAggGroupCommon(service, schema, stmt, rrs, mysqlSelectQuery, tc);
    }

    private void parseAggExprCommon(SchemaConfig schema, RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery, List<SQLSelectItem> selectColumns, Map<String, String> aliaColumns, BaseTableConfig tc, boolean isDistinct) throws SQLException {
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
                    addToAliaColumn(selectColumns, aliaColumns, selectItem);
                }
            } else if (itemExpr instanceof SQLAllColumnExpr || (itemExpr instanceof SQLPropertyExpr && ((SQLPropertyExpr) itemExpr).getName().equals("*"))) {
                TableMeta tbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schema.getName(), tc.getName());
                if (tbMeta == null) {
                    String msg = "Meta data of table '" + schema.getName() + "." + tc.getName() + "' doesn't exist";
                    LOGGER.info(msg);
                    throw new SQLNonTransientException(msg);
                }
                for (ColumnMeta column : tbMeta.getColumns()) {
                    aliaColumns.put(column.getName(), column.getName());

                    selectColumns.add(new SQLSelectItem(new SQLIdentifierExpr(column.getName())));
                }
            } else {
                if (isDistinct && !isNeedOptimizer(itemExpr)) {
                    if (itemExpr instanceof SQLIdentifierExpr) {
                        SQLIdentifierExpr item = (SQLIdentifierExpr) itemExpr;
                        if (hasShardingColumn(tc, item.getSimpleName())) hasPartitionColumn = true;
                        addToAliaColumn(selectColumns, aliaColumns, selectItem);
                    } else if (itemExpr instanceof SQLPropertyExpr) {
                        SQLPropertyExpr item = (SQLPropertyExpr) itemExpr;
                        if (hasShardingColumn(tc, item.getSimpleName())) hasPartitionColumn = true;
                        addToAliaColumn(selectColumns, aliaColumns, selectItem);
                    }
                } else if (isSumFuncOrSubQuery(schema.getName(), itemExpr)) {
                    rrs.setNeedOptimizer(true);
                    return;
                } else {
                    addToAliaColumn(selectColumns, aliaColumns, selectItem);
                }
            }
        }
        if (isDistinct && !hasPartitionColumn) {
            rrs.setNeedOptimizer(true);
            return;
        }
        parseGroupCommon(rrs, mysqlSelectQuery, aliaColumns, tc);
    }

    private void parseGroupCommon(RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery, Map<String, String> aliaColumns, BaseTableConfig tc) {
        if (mysqlSelectQuery.getGroupBy() != null) {
            SQLSelectGroupByClause groupBy = mysqlSelectQuery.getGroupBy();
            SQLExpr partitionColumn = null;
            for (SQLExpr groupByItem : groupBy.getItems()) {
                if (isNeedOptimizer(groupByItem)) {
                    rrs.setNeedOptimizer(true);
                    return;
                } else if (groupByItem instanceof SQLIdentifierExpr) {
                    SQLIdentifierExpr item = (SQLIdentifierExpr) groupByItem;
                    if (hasShardingColumnWithAlia(tc, StringUtil.removeBackQuote(item.getSimpleName()), aliaColumns)) {
                        partitionColumn = item;
                        break;
                    }
                } else if (groupByItem instanceof SQLPropertyExpr) {
                    SQLPropertyExpr item = (SQLPropertyExpr) groupByItem;
                    if (hasShardingColumnWithAlia(tc, StringUtil.removeBackQuote(item.getSimpleName()), aliaColumns)) {
                        partitionColumn = item;
                        break;
                    }
                }
            }
            if (groupBy.getItems().size() > 0 && partitionColumn == null) {
                rrs.setNeedOptimizer(true);
                return;
            }
            if (groupBy.getItems().size() == 0 && groupBy.getHaving() != null) {
                // only having filter need optimizer
                rrs.setNeedOptimizer(true);
                return;
            }
            rrs.setGroupByColsHasShardingCols(partitionColumn != null);
        }
    }

    private Set<SQLSelectItem> groupColumnPushSelectList(List<SQLExpr> groupByItemList, List<SQLSelectItem> selectColumns) {
        Set<SQLSelectItem> pushItem = new HashSet<>();

        for (SQLExpr groupByItem : groupByItemList) {
            String groupColumnName = null;
            if (groupByItem instanceof SQLIdentifierExpr) {
                groupColumnName = ((SQLIdentifierExpr) groupByItem).getSimpleName();
            } else if (groupByItem instanceof SQLPropertyExpr) {
                groupColumnName = ((SQLPropertyExpr) groupByItem).getSimpleName();
            }
            if (!StringUtil.isEmpty(groupColumnName)) {
                if (!hasColumnOrAlia(StringUtil.removeBackQuote(groupColumnName), selectColumns)) {
                    pushItem.add(new SQLSelectItem(groupByItem));
                }
            }
        }
        return pushItem;
    }

    private boolean hasColumnOrAlia(String columnName, List<SQLSelectItem> selectColumns) {
        return selectColumns.stream().anyMatch(s -> (s.getAlias() != null && StringUtil.removeBackQuote(s.getAlias()).equalsIgnoreCase(columnName)) ||
                ((s.getExpr() instanceof SQLPropertyExpr) && StringUtil.removeBackQuote(((SQLPropertyExpr) s.getExpr()).getSimpleName()).equalsIgnoreCase(columnName)) ||
                StringUtil.removeBackQuote(s.getExpr().toString()).equalsIgnoreCase(columnName));
    }

    private boolean hasShardingColumn(BaseTableConfig tc, String columnName) {
        return tc instanceof ShardingTableConfig && columnName.equalsIgnoreCase(((ShardingTableConfig) tc).getShardingColumn());
    }

    private boolean hasShardingColumnWithAlia(BaseTableConfig tc, String columnName, Map<String, String> aliaColumns) {
        String shardingColumn = "";
        boolean isShardingColumn = tc instanceof ShardingTableConfig && columnName.equalsIgnoreCase(shardingColumn = ((ShardingTableConfig) tc).getShardingColumn());
        if (!isShardingColumn) {
            String finalShardingColumn = shardingColumn;
            Optional<Map.Entry<String, String>> alias = aliaColumns.entrySet().stream().filter(c -> c.getKey().toUpperCase().equals(finalShardingColumn)).findFirst();
            if (alias.isPresent()) {
                isShardingColumn = tc instanceof ShardingTableConfig && alias.get().getValue().equalsIgnoreCase(columnName);
            }
        }
        return isShardingColumn;
    }

    private boolean isSumFuncOrSubQuery(String schema, SQLExpr itemExpr) {
        MySQLItemVisitor ev = new MySQLItemVisitor(schema, CharsetUtil.getCharsetDefaultIndex("utf8mb4"), ProxyMeta.getInstance().getTmManager(), null, null);
        itemExpr.accept(ev);
        Item selItem = ev.getItem();
        return containSumFuncOrSubQuery(selItem);
    }

    private boolean containSumFuncOrSubQuery(Item selItem) {
        if (selItem.isWithSumFunc()) {
            return true;
        }
        if (selItem.isWithSubQuery()) {
            return true;
        }
        if (selItem.getArgCount() > 0) {
            for (Item child : selItem.arguments()) {
                if (containSumFuncOrSubQuery(child)) {
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

    private void addToAliaColumn(List<SQLSelectItem> selectColumns, Map<String, String> aliaColumns, SQLSelectItem item) {
        String alia = item.getAlias();
        String field = getFieldName(item);
        if (alia == null) {
            alia = field;
        }
        aliaColumns.put(field, alia);
        selectColumns.add(item);
    }

    private String getFieldName(SQLSelectItem item) {
        if ((item.getExpr() instanceof SQLPropertyExpr) || (item.getExpr() instanceof SQLMethodInvokeExpr) ||
                (item.getExpr() instanceof SQLIdentifierExpr) || item.getExpr() instanceof SQLBinaryOpExpr) {
            return item.getExpr().toString(); // alias
        } else {
            return item.toString();
        }
    }

    private void parseAggGroupCommon(ShardingService service, SchemaConfig schema, SQLStatement stmt, RouteResultset rrs,
                                     MySqlSelectQueryBlock mysqlSelectQuery, BaseTableConfig tc) throws SQLException {
        Map<String, String> aliaColumns = new HashMap<>();
        List<SQLSelectItem> selectColumns = new LinkedList<>();
        boolean isDistinct = (mysqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCT) || (mysqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCTROW);
        parseAggExprCommon(schema, rrs, mysqlSelectQuery, selectColumns, aliaColumns, tc, isDistinct);
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

        boolean isGroupByColPushSelectList = tryGroupColumnPushSelectList(aliaColumns, selectColumns,
                mysqlSelectQuery, rrs, service.getCharset().getResultsIndex());

        if (isDistinct) {
            String sql = RouterUtil.removeSchema(statementToString(stmt), schema.getName());
            rrs.changeNodeSqlAfterAddLimit(sql, 0, -1);
        } else if (isGroupByColPushSelectList) {
            String sql = RouterUtil.removeSchema(statementToString(stmt), schema.getName());
            rrs.changeNodeSqlAfterAddLimit(sql, rrs.getLimitStart(), rrs.getLimitSize());
        }
    }

    /**
     * when fakeMysqlVersion is 8.0, in 'group by' no longer has the semantics of 'order by'
     */
    private boolean tryGroupColumnPushSelectList(Map<String, String> aliaColumns, List<SQLSelectItem> selectColumns,
                                                 MySqlSelectQueryBlock mysqlSelectQuery, RouteResultset rrs, int charsetIndex) {
        boolean isGroupByColPushSelectList = false;
        if (!VersionUtil.isMysql8(SystemConfig.getInstance().getFakeMySQLVersion()) &&
                rrs.isGroupByColsHasShardingCols()) { // && mysqlSelectQuery.getGroupBy() != null

            Set<SQLSelectItem> pushSelectList = groupColumnPushSelectList(mysqlSelectQuery.getGroupBy().getItems(), selectColumns);
            if (!CollectionUtil.isEmpty(pushSelectList)) {
                for (SQLSelectItem e : pushSelectList) {
                    mysqlSelectQuery.getSelectList().add(e);
                }
                isGroupByColPushSelectList = true;
            }

            // setGroupByCols
            List<SQLExpr> groupByItems = mysqlSelectQuery.getGroupBy().getItems();
            String[] groupByCols = buildGroupByCols(groupByItems, aliaColumns);
            rrs.setGroupByCols(groupByCols);

            if (isGroupByColPushSelectList) {
                rrs.setSelectCols(
                        handleSelectItems(selectColumns, rrs, charsetIndex));
            }
        }
        return isGroupByColPushSelectList;
    }

    private LinkedList<Item> handleSelectItems(List<SQLSelectItem> selectList, RouteResultset rrs, int charsetIndex) {
        LinkedList<Item> selectItems = new LinkedList<>();
        String tableName = rrs.getTableAlias() != null ? rrs.getTableAlias() : rrs.getTable();
        for (SQLSelectItem sel : selectList) {
            String tName;
            String cName;
            if (sel.getExpr() instanceof SQLPropertyExpr) {
                if (sel.getAlias() != null) {
                    tName = tableName;
                    cName = sel.getAlias();
                } else {
                    SQLPropertyExpr seli = (SQLPropertyExpr) sel.getExpr();
                    if (seli.getOwner() instanceof SQLPropertyExpr) {
                        tName = ((SQLPropertyExpr) seli.getOwner()).getSimpleName();
                    } else {
                        tName = seli.getOwner().toString();
                    }
                    cName = seli.getSimpleName();
                }
            } else {
                tName = tableName;
                cName = sel.getAlias() != null ? sel.getAlias() : sel.getExpr().toString();
            }
            ItemField selItem = new ItemField(rrs.getSchema(), StringUtil.removeBackQuote(tName), StringUtil.removeBackQuote(cName), charsetIndex);
            selItem.setAlias(sel.getAlias() == null ? null : StringUtil.removeBackQuote(sel.getAlias()));
            selItem.setCharsetIndex(charsetIndex);
            selectItems.add(selItem);
        }
        return selectItems;
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
                column = ((SQLPropertyExpr) sqlExpr).getSimpleName();
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
            groupByCols[i] = getAliaColumn(aliaColumns, StringUtil.removeBackQuote(column)); // column;
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
            return routeToNoSharding(schema, rrs, schemas, noShardingNode, null);
        } else if (schemas.size() > 0 && SchemaUtil.MYSQL_SYS_SCHEMA.containsAll(schemas)) {
            MysqlSystemSchemaHandler.handle(service, null, selectStmt.getSelect().getQuery());
            rrs.setFinishedExecute(true);
            return schema;
        } else if (containsInnerFunction) {
            rrs.setNeedOptimizer(true);
            rrs.setSqlStatement(selectStmt);
            return schema;
        } else {
            if (rrs.isRoutePenetration()) {
                LOGGER.debug("the query {} match the route penetration regex", rrs.getSrcStatement());
                rrs = tryDirectRoute(schema, rrs);
                if (rrs.isFinishedRoute()) {
                    LOGGER.debug("the query {} match the route penetration rule, will direct route", rrs.getSrcStatement());
                    return schema;
                }
            }
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
                limit.setRowCount(new SQLIntegerExpr(needAddLimitSize));
                mysqlSelectQuery.setLimit(limit);
                rrs.setLimitSize(needAddLimitSize);
                String sql = RouterUtil.removeSchema(statementToString(stmt), sqlSchema.getName());
                rrs.changeNodeSqlAfterAddLimit(sql, 0, needAddLimitSize);
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
        } else if (mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode() || mysqlSelectQuery.isForShare()) {
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
