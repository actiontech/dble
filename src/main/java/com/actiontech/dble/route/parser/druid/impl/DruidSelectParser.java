/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerPrivileges;
import com.actiontech.dble.config.ServerPrivileges.Checktype;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.plan.visitor.MySQLItemVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.handler.MysqlInformationSchemaHandler;
import com.actiontech.dble.server.handler.MysqlProcHandler;
import com.actiontech.dble.server.response.InformationSchemaProfiling;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.sqlengine.mpp.ColumnRoutePair;
import com.actiontech.dble.sqlengine.mpp.HavingCols;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery;
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
                                     ServerSchemaStatVisitor visitor, ServerConnection sc) throws SQLException {
        SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
        String schemaName = schema == null ? null : schema.getName();
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) sqlSelectQuery;
            if (mysqlSelectQuery.getInto() != null) {
                throw new SQLNonTransientException("select ... into is not supported!");
            }
            checkSelectList(mysqlSelectQuery);
            SQLTableSource mysqlFrom = mysqlSelectQuery.getFrom();
            if (mysqlFrom == null) {
                RouterUtil.routeNoNameTableToSingleNode(rrs, schema);
                return schema;
            }
            SchemaInfo schemaInfo;
            if (mysqlFrom instanceof SQLExprTableSource) {
                SQLExprTableSource fromSource = (SQLExprTableSource) mysqlFrom;
                schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, fromSource);
                if (schemaInfo.isDual()) {
                    RouterUtil.routeNoNameTableToSingleNode(rrs, schema);
                    return schema;
                }
                if (matchSysTable(rrs, sc, schemaInfo)) {
                    return schema;
                }

                if (schemaInfo.getSchemaConfig() == null) {
                    String msg = "No Supported, sql:" + stmt;
                    throw new SQLNonTransientException(msg);
                }
                if (!ServerPrivileges.checkPrivilege(sc, schemaInfo.getSchema(), schemaInfo.getTable(), Checktype.SELECT)) {
                    String msg = "The statement DML privilege check is not passed, sql:" + stmt;
                    throw new SQLNonTransientException(msg);
                }
                rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
                schema = schemaInfo.getSchemaConfig();
                if (RouterUtil.isNoSharding(schema, schemaInfo.getTable())) {
                    RouterUtil.routeToSingleNode(rrs, schema.getDataNode());
                    return schema;
                }

                TableConfig tc = schema.getTables().get(schemaInfo.getTable());
                if (tc == null) {
                    String msg = "Table '" + schema.getName() + "." + schemaInfo.getTable() + "' doesn't exist";
                    throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
                }

                super.visitorParse(schema, rrs, stmt, visitor, sc);
                if (visitor.isHasSubQuery()) {
                    rrs.setSqlStatement(selectStmt);
                    rrs.setNeedOptimizer(true);
                    return schema;
                }
                parseOrderAggGroupMysql(schema, stmt, rrs, mysqlSelectQuery, tc);
                // select ...for update /in shard mode /in transaction
                if ((mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isLockInShareMode()) && !sc.isAutocommit()) {
                    rrs.setCanRunInReadDB(false);
                }
            } else if (mysqlFrom instanceof SQLSubqueryTableSource ||
                    mysqlFrom instanceof SQLJoinTableSource ||
                    mysqlFrom instanceof SQLUnionQueryTableSource) {
                return executeComplexSQL(schemaName, schema, rrs, selectStmt, sc);
            }
        } else if (sqlSelectQuery instanceof MySqlUnionQuery) {
            return executeComplexSQL(schemaName, schema, rrs, selectStmt, sc);
        }

        return schema;
    }

    private void checkSelectList(MySqlSelectQueryBlock mysqlSelectQuery) throws SQLNonTransientException {
        for (SQLSelectItem item : mysqlSelectQuery.getSelectList()) {
            if (item.getExpr() instanceof SQLQueryExpr) {
                throw new SQLNonTransientException("query statement as column is not supported!");
            }
        }
    }

    private boolean matchSysTable(RouteResultset rrs, ServerConnection sc, SchemaInfo schemaInfo) {
        // support PhpAdmin
        //TODO:refactor INFORMATION_SCHEMA,MYSQL
        if (SchemaUtil.INFORMATION_SCHEMA.equals(schemaInfo.getSchema())) {
            MysqlInformationSchemaHandler.handle(schemaInfo, sc);
            rrs.setFinishedExecute(true);
            return true;
        }

        if (SchemaUtil.MYSQL_SCHEMA.equals(schemaInfo.getSchema()) &&
                SchemaUtil.TABLE_PROC.equals(schemaInfo.getTable())) {
            // support MySQLWorkbench
            MysqlProcHandler.handle(sc);
            rrs.setFinishedExecute(true);
            return true;
        }
        // fix navicat SELECT STATE AS `State`, ROUND(SUM(DURATION),7) AS
        // `Duration`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS
        // `Percentage` FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID=
        // GROUP BY STATE ORDER BY SEQ
        if (SchemaUtil.INFORMATION_SCHEMA.equals(schemaInfo.getSchema()) &&
                SchemaUtil.TABLE_PROFILING.equals(schemaInfo.getTable()) &&
                rrs.getStatement().toUpperCase().contains("CONCAT(ROUND(SUM(DURATION)/*100,3)")) {
            InformationSchemaProfiling.response(sc);
            rrs.setFinishedExecute(true);
            return true;
        }
        return false;
    }

    private void parseOrderAggGroupMysql(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs,
                                         MySqlSelectQueryBlock mysqlSelectQuery, TableConfig tc) throws SQLException {
        //simple merge of ORDER BY has bugs,so optimizer here
        if (mysqlSelectQuery.getOrderBy() != null) {
            tryAddLimit(schema, tc, mysqlSelectQuery);
            rrs.setSqlStatement(stmt);
            rrs.setNeedOptimizer(true);
            return;
        }
        parseAggGroupCommon(schema, stmt, rrs, mysqlSelectQuery, tc);
    }

    private void parseAggExprCommon(SchemaConfig schema, RouteResultset rrs, MySqlSelectQueryBlock mysqlSelectQuery, Map<String, String> aliaColumns, TableConfig tc) throws SQLException {
        List<SQLSelectItem> selectList = mysqlSelectQuery.getSelectList();
        for (SQLSelectItem item : selectList) {
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
                String methodName = ((SQLMethodInvokeExpr) itemExpr).getMethodName().toUpperCase();
                if (aggregateSet.contains(methodName)) {
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
                StructureMeta.TableMeta tbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schema.getName(), tc.getName());
                if (tbMeta == null) {
                    String msg = "Meta data of table '" + schema.getName() + "." + tc.getName() + "' doesn't exist";
                    LOGGER.warn(msg);
                    throw new SQLNonTransientException(msg);
                }
                for (StructureMeta.ColumnMeta column : tbMeta.getColumnsList()) {
                    aliaColumns.put(column.getName(), column.getName());
                }
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
            if (!hasPartitionColumn) {
                rrs.setNeedOptimizer(true);
            }
        }
    }

    private boolean isSumFunc(String schema, SQLExpr itemExpr) {
        MySQLItemVisitor ev = new MySQLItemVisitor(schema);
        itemExpr.accept(ev);
        Item selItem = ev.getItem();
        return contactSumFunc(selItem);
    }

    private boolean contactSumFunc(Item selItem) {
        if (selItem.isWithSumFunc()) {
            return true;
        }
        if (selItem.getArgCount() > 0) {
            for (Item child : selItem.arguments()) {
                if (contactSumFunc(child)) {
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

    private Map<String, String> parseAggGroupCommon(SchemaConfig schema, SQLStatement stmt, RouteResultset rrs,
                                                    MySqlSelectQueryBlock mysqlSelectQuery, TableConfig tc) throws SQLException {
        Map<String, String> aliaColumns = new HashMap<>();

        parseAggExprCommon(schema, rrs, mysqlSelectQuery, aliaColumns, tc);
        if (rrs.isNeedOptimizer()) {
            tryAddLimit(schema, tc, mysqlSelectQuery);
            rrs.setSqlStatement(stmt);
            return aliaColumns;
        }

        // distinct change to group by
        boolean isNeedChangeSql = (mysqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCT) || (mysqlSelectQuery.getDistionOption() == SQLSetQuantifier.DISTINCTROW);
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
        }

        if (isNeedChangeSql) {
            rrs.changeNodeSqlAfterAddLimit(stmt.toString(), 0, -1);
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
            leftValue = ((SQLAggregateExpr) left).getMethodName() + "(" +
                    ((SQLAggregateExpr) left).getArguments().get(0) + ")";
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

    private SchemaConfig executeComplexSQL(String schemaName, SchemaConfig schema, RouteResultset rrs, SQLSelectStatement selectStmt, ServerConnection sc)
            throws SQLException {
        StringPtr sqlSchema = new StringPtr(null);
        if (!SchemaUtil.isNoSharding(sc, selectStmt.getSelect().getQuery(), selectStmt, schemaName, sqlSchema)) {
            rrs.setSqlStatement(selectStmt);
            rrs.setNeedOptimizer(true);
            return schema;
        } else {
            String realSchema = sqlSchema.get() == null ? schemaName : sqlSchema.get();
            SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(realSchema);
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), realSchema));
            RouterUtil.routeToSingleNode(rrs, schemaConfig.getDataNode());
            return schemaConfig;
        }
    }

    /**
     * changeSql: add limit if need
     */
    @Override
    public void changeSql(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, LayerCachePool cachePool)
            throws SQLException {
        if (rrs.isFinishedRoute() || rrs.isFinishedExecute() || rrs.isNeedOptimizer()) {
            return;
        }
        tryRouteSingleTable(schema, rrs, cachePool);
        rrs.copyLimitToNodes();
        SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) selectStmt.getSelect().getQuery();
            int limitStart = 0;
            int limitSize = schema.getDefaultMaxLimit();

            Map<String, Map<String, Set<ColumnRoutePair>>> allConditions = getAllConditions();
            boolean isNeedAddLimit = isNeedAddLimit(schema, rrs, mysqlSelectQuery, allConditions);
            if (isNeedAddLimit) {
                SQLLimit limit = new SQLLimit();
                limit.setRowCount(new SQLIntegerExpr(limitSize));
                mysqlSelectQuery.setLimit(limit);
                rrs.setLimitSize(limitSize);
                String sql = getSql(rrs, stmt, isNeedAddLimit, schema.getName());
                rrs.changeNodeSqlAfterAddLimit(sql, 0, limitSize);

            }
            SQLLimit limit = mysqlSelectQuery.getLimit();
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
                    String sql = getSql(rrs, stmt, isNeedAddLimit, schema.getName());
                    rrs.changeNodeSqlAfterAddLimit(sql, 0, limitStart + limitSize);
                } else {
                    rrs.changeNodeSqlAfterAddLimit(rrs.getStatement(), rrs.getLimitStart(), rrs.getLimitSize());
                }
            }
            rrs.setCacheAble(isNeedCache(schema));
        }

    }

    private void tryRouteSingleTable(SchemaConfig schema, RouteResultset rrs, LayerCachePool cachePool)
            throws SQLException {
        if (rrs.isFinishedRoute()) {
            return;
        }
        SortedSet<RouteResultsetNode> nodeSet = new TreeSet<>();
        String table = ctx.getTables().get(0);
        if (RouterUtil.isNoSharding(schema, table)) {
            RouterUtil.routeToSingleNode(rrs, schema.getDataNode());
            return;
        }
        for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
            RouteResultset rrsTmp = RouterUtil.tryRouteForOneTable(schema, unit, table, rrs, true, cachePool);
            if (rrsTmp != null && rrsTmp.getNodes() != null) {
                Collections.addAll(nodeSet, rrsTmp.getNodes());
                if (rrsTmp.isGlobalTable()) {
                    break;
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
        for (RouteResultsetNode aNodeSet : nodeSet) {
            nodes[i] = aNodeSet;
            i++;
        }

        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }

    /**
     * getAllConditions
     */
    private Map<String, Map<String, Set<ColumnRoutePair>>> getAllConditions() {
        Map<String, Map<String, Set<ColumnRoutePair>>> map = new HashMap<>();
        for (RouteCalculateUnit unit : ctx.getRouteCalculateUnits()) {
            if (unit != null && unit.getTablesAndConditions() != null) {
                map.putAll(unit.getTablesAndConditions());
            }
        }

        return map;
    }


    protected String getSql(RouteResultset rrs, SQLStatement stmt, boolean isNeedAddLimit, String schema) {
        if ((isNeedChangeLimit(rrs) || isNeedAddLimit)) {
            return RouterUtil.removeSchema(stmt.toString(), schema);
        }
        return rrs.getStatement();
    }


    private boolean isNeedChangeLimit(RouteResultset rrs) {
        if (rrs.getNodes() == null) {
            return false;
        } else {
            return rrs.getNodes().length > 1;
        }
    }

    private boolean isNeedCache(SchemaConfig schema) {
        if (ctx.getTables() == null || ctx.getTables().size() == 0) {
            return false;
        }
        TableConfig tc = schema.getTables().get(ctx.getTables().get(0));
        if (tc == null || (ctx.getTables().size() == 1 && tc.isGlobalTable())) {
            return false;
        } else {
            //single table
            if (ctx.getTables().size() == 1) {
                String tableName = ctx.getTables().get(0);
                String primaryKey = schema.getTables().get(tableName).getPrimaryKey();
                if (ctx.getRouteCalculateUnit().getTablesAndConditions().get(tableName) != null &&
                        ctx.getRouteCalculateUnit().getTablesAndConditions().get(tableName).get(primaryKey) != null &&
                        tc.getDataNodes().size() > 1) { //primaryKey condition
                    return false;
                }
            }
            return true;
        }
    }

    private void tryAddLimit(SchemaConfig schema, TableConfig tableConfig,
                             MySqlSelectQueryBlock mysqlSelectQuery) {
        if (schema.getDefaultMaxLimit() == -1) {
            return;
        } else if (mysqlSelectQuery.getLimit() != null) {
            return;
        } else if (!tableConfig.isNeedAddLimit()) {
            return;
        }
        SQLLimit limit = new SQLLimit();
        limit.setRowCount(new SQLIntegerExpr(schema.getDefaultMaxLimit()));
        mysqlSelectQuery.setLimit(limit);
    }

    /**
     * @param schema
     * @param rrs
     * @param mysqlSelectQuery
     * @param allConditions
     * @return
     */
    private boolean isNeedAddLimit(SchemaConfig schema, RouteResultset rrs,
                                   MySqlSelectQueryBlock mysqlSelectQuery, Map<String, Map<String, Set<ColumnRoutePair>>> allConditions) {
        if (rrs.getLimitSize() > -1) {
            return false;
        } else if (schema.getDefaultMaxLimit() == -1) {
            return false;
        } else if (mysqlSelectQuery.getLimit() != null) { // has already limit
            return false;
        } else if (ctx.getTables().size() == 1) {
            if (rrs.hasPrimaryKeyToCache()) {
                // single table and has primary key , need not limit because of only one row
                return false;
            }
            String tableName = ctx.getTables().get(0);
            TableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig == null) {
                return schema.getDefaultMaxLimit() > -1; // get schema's configure
            }

            boolean isNeedAddLimit = tableConfig.isNeedAddLimit();
            if (!isNeedAddLimit) {
                return false; // get table configure
            }

            if (schema.getTables().get(tableName).isGlobalTable()) {
                return true;
            }

            String primaryKey = schema.getTables().get(tableName).getPrimaryKey();
            // no condition
            return allConditions.get(tableName) == null || allConditions.get(tableName).get(primaryKey) == null;
        } else { // no table or multi-table
            return false;
        }

    }

}
