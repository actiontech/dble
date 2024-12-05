/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.route.parser.druid.impl;


import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.ChildTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.GlobalTableConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.ShardingTableConfig;
import com.oceanbase.obsharding_d.config.privileges.ShardingPrivileges;
import com.oceanbase.obsharding_d.config.privileges.ShardingPrivileges.CheckType;
import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.plan.common.ptr.StringPtr;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.route.parser.druid.ServerSchemaStatVisitor;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil.SchemaInfo;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.singleton.SequenceManager;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.collect.Sets;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * Created by szf on 2017/8/18.
 */
public class DruidReplaceParser extends DruidInsertReplaceParser {

    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain)
            throws SQLException {
        //data & object prepare
        SQLReplaceStatement replace = (SQLReplaceStatement) stmt;
        String schemaName = schema == null ? null : schema.getName();
        SQLExprTableSource tableSource = replace.getTableSource();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, tableSource);

        //privilege check
        if (!ShardingPrivileges.checkPrivilege(service.getUserConfig(), schemaInfo.getSchema(), schemaInfo.getTable(), CheckType.INSERT)) {
            String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
            throw new SQLNonTransientException(msg);
        }

        //No sharding table check
        schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        if (parserNoSharding(service, schemaName, schemaInfo, rrs, replace)) {
            return schema;
        }
        if (replace.getQuery() != null) {
            tryRouteInsertQuery(service, rrs, stmt, visitor, schemaInfo);
            return schema;
        }

        //check the config of target table
        BaseTableConfig tc = schema.getTables().get(tableName);
        checkTableExists(tc, schema.getName(), tableName, CheckType.INSERT);

        //if the target table is global table than
        if (tc instanceof GlobalTableConfig) {
            String sql = rrs.getStatement();
            sql = RouterUtil.removeSchema(sql, schemaInfo.getSchema());
            rrs.setStatement(sql);
            if (ctx.getTables().isEmpty()) {
                RouterUtil.routeToMultiNode(false, rrs, tc.getShardingNodes(), true, Sets.newHashSet(schemaName + "." + tableName));
            } else {
                RouterUtil.routeToMultiNode(false, rrs, tc.getShardingNodes(), true, ctx.getTables());
            }
            rrs.setFinishedRoute(true);
            return schema;
        } else if (tc instanceof ChildTableConfig) { // insert childTable will finished router while parser
            ChildTableConfig child = (ChildTableConfig) tc;
            if (child.getIncrementColumn() != null) {
                replace = genNewSqlReplaceStatement(rrs, replace, schemaInfo, child.getIncrementColumn(), service);
            }
            parserChildTable(schemaInfo, rrs, replace, service, isExplain);
            return schema;
        } else if (tc instanceof ShardingTableConfig) {
            ShardingTableConfig tableConfig = (ShardingTableConfig) tc;
            if (tableConfig.getIncrementColumn() != null) {
                replace = genNewSqlReplaceStatement(rrs, replace, schemaInfo, tableConfig.getIncrementColumn(), service);
            }
            String partitionColumn = tableConfig.getShardingColumn();
            if (isMultiReplace(replace)) {
                parserBatchInsert(schemaInfo, rrs, partitionColumn, replace, service.getCharset().getClient());
            } else {
                parserSingleInsert(schemaInfo, rrs, partitionColumn, replace, service.getCharset().getClient());
            }
        } else {
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            ctx.addTable(new Pair<>(schema.getName(), tableName));
        }

        return schema;
    }

    private SQLReplaceStatement genNewSqlReplaceStatement(RouteResultset rrs, SQLReplaceStatement replace, SchemaInfo schemaInfo, String incrementColumn, FrontendService service) throws SQLNonTransientException {
        SQLStatement stmt;
        String sql = changeReplaceSQLByIncrement(schemaInfo, replace, rrs.getStatement(), incrementColumn, service);
        rrs.setStatement(sql);
        SQLStatementParser parser = new MySqlStatementParser(sql);
        stmt = parser.parseStatement();
        replace = (SQLReplaceStatement) stmt;
        return replace;
    }

    @Override
    SQLSelect acceptVisitor(SQLObject stmt, ServerSchemaStatVisitor visitor) {
        SQLReplaceStatement replace = (SQLReplaceStatement) stmt;
        replace.getQuery().accept(visitor);
        return replace.getQuery().getSubQuery();
    }

    @Override
    int tryGetShardingColIndex(SchemaInfo schemaInfo, SQLStatement stmt, String partitionColumn) throws SQLNonTransientException {
        return tryGetShardingColIndex(schemaInfo, (SQLReplaceStatement) stmt, partitionColumn);
    }

    /**
     * find the index of the key in column list
     *
     * @param schemaInfo      ManagerSchemaInfo
     * @param replaceStmt     SQLReplaceStatement
     * @param partitionColumn partitionColumn
     * @return the index of the partition column
     * @throws SQLNonTransientException if not find
     */
    private int tryGetShardingColIndex(SchemaInfo schemaInfo, SQLReplaceStatement replaceStmt, String partitionColumn) throws SQLNonTransientException {
        int shardingColIndex = getShardingColIndex(schemaInfo, replaceStmt.getColumns(), partitionColumn);
        if (shardingColIndex != -1) return shardingColIndex;
        throw new SQLNonTransientException("bad insert sql, shardingColumn/joinColumn:" + partitionColumn + " not provided," + replaceStmt);
    }

    /**
     * check if the nosharding tables are Involved
     */
    private boolean parserNoSharding(ShardingService service, String contextSchema, SchemaInfo schemaInfo, RouteResultset rrs, SQLReplaceStatement replace) throws SQLException {
        String noShardingNode = RouterUtil.isNoSharding(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
        if (noShardingNode != null) {
            StringPtr noShardingNodePr = new StringPtr(noShardingNode);
            Set<String> schemas = new HashSet<>();
            if (replace.getQuery() != null) {
                //replace into ...select  if the both table is nosharding table
                SQLSelect select = replace.getQuery().getSubQuery();
                SQLSelectStatement selectStmt = new SQLSelectStatement(select);
                if (!SchemaUtil.isNoSharding(service, select.getQuery(), replace, selectStmt, contextSchema, schemas, noShardingNodePr)) {
                    return false;
                }
            }
            routeToNoSharding(schemaInfo.getSchemaConfig(), rrs, schemas, noShardingNodePr, schemaInfo.getTable());
            return true;
        }
        return false;
    }


    private String changeReplaceSQLByIncrement(SchemaInfo schemaInfo, SQLReplaceStatement replace, String originSql, String incrementColumn, FrontendService service) throws SQLNonTransientException {
        TableMeta orgTbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(),
                schemaInfo.getTable());
        if (orgTbMeta == null)
            return originSql;

        String tableName = schemaInfo.getTable();

        StringBuilder sb = new StringBuilder(200/* this is to improve the performance) */).append("replace into ").append(tableName);

        List<SQLExpr> columns = replace.getColumns();

        int autoIncrement = -1;
        int idxGlobal = -1;
        int colSize;
        // replace with no column name ：replace into t values(xxx,xxx)
        if (columns == null || columns.size() <= 0) {
            autoIncrement = getIncrementKeyIndex(schemaInfo, incrementColumn);
            colSize = orgTbMeta.getColumns().size();
        } else { // replace sql with  column names
            boolean hasIncrementInSql = containsIncrementColumns(incrementColumn, sb, columns);
            colSize = columns.size();
            if (!hasIncrementInSql) {
                getIncrementKeyIndex(schemaInfo, incrementColumn);
                autoIncrement = columns.size();
                sb.append(",").append(incrementColumn);
                colSize++;
            }
            sb.append(")");
        }

        sb.append(" values");
        String tableKey = StringUtil.getFullName(schemaInfo.getSchema(), schemaInfo.getTable());
        List<SQLInsertStatement.ValuesClause> vcl = replace.getValuesList();
        if (vcl != null && vcl.size() > 1) { // batch insert
            for (int j = 0; j < vcl.size(); j++) {
                if (j != vcl.size() - 1)
                    appendValues(tableKey, vcl.get(j).getValues(), sb, autoIncrement, idxGlobal, colSize, service).append(",");
                else
                    appendValues(tableKey, vcl.get(j).getValues(), sb, autoIncrement, idxGlobal, colSize, service);
            }
        } else { // single line insert
            List<SQLExpr> values = replace.getValuesList().get(0).getValues();
            appendValues(tableKey, values, sb, autoIncrement, idxGlobal, colSize, service);
        }

        return RouterUtil.removeSchema(sb.toString(), schemaInfo.getSchema());
    }

    private boolean containsIncrementColumns(String incrementColumn, StringBuilder sb, List<SQLExpr> columns) {
        sb.append("(");
        boolean hasIncrementInSql = false;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).toString().equalsIgnoreCase(incrementColumn)) {
                hasIncrementInSql = true;
            }
            if (i < columns.size() - 1)
                sb.append(columns.get(i).toString()).append(",");
            else
                sb.append(columns.get(i).toString());
        }
        return hasIncrementInSql;
    }


    private static StringBuilder appendValues(String tableKey, List<SQLExpr> values, StringBuilder sb,
                                              int autoIncrement, int idxGlobal, int colSize, FrontendService service) throws SQLNonTransientException {
        // check the value number & the column number is all right
        int size = values.size();
        int checkSize = colSize - (idxGlobal < 0 ? 0 : 1);
        int lowerlimit = colSize - (autoIncrement < 0 ? 0 : 1) - (idxGlobal < 0 ? 0 : 1);
        if (checkSize < size && idxGlobal >= 0) {
            String msg = "In insert Syntax, you can't set value for Global check column!";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        } else if (size < lowerlimit) {
            String msg = "Column count doesn't match value count";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }

        sb.append("(");
        int iValue = 0;
        //put the value number into string buffer
        for (int i = 0; i < colSize; i++) {
            if (i == idxGlobal) {
                sb.append(new Date().getTime());
            } else if (i == autoIncrement) {
                if (checkSize > size) {
                    long id = SequenceManager.nextId(tableKey, service);
                    sb.append(id);
                } else {
                    String value = SQLUtils.toMySqlString(values.get(iValue++));
                    sb.append(value);
                }
            } else {
                String value = SQLUtils.toMySqlString(values.get(iValue++));
                sb.append(value);
            }
            if (i < colSize - 1) {
                sb.append(",");
            }
        }
        return sb.append(")");
    }

    private void parserChildTable(SchemaInfo schemaInfo, final RouteResultset rrs, SQLReplaceStatement replace, final ShardingService service, boolean isExplain) throws SQLNonTransientException {
        final SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        final ChildTableConfig tc = (ChildTableConfig) (schema.getTables().get(tableName));
        //check if the childtable replace with the multi
        if (isMultiReplace(replace)) {
            String msg = "ChildTable multi insert not provided";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        //find the value of child table join key
        String joinColumn = tc.getJoinColumn();
        int joinColumnIndex = getJoinColumnIndex(schemaInfo, replace, joinColumn);
        final String joinColumnVal = replace.getValuesList().get(0).getValues().get(joinColumnIndex).toString();
        String realVal = StringUtil.removeApostrophe(joinColumnVal);
        final String sql = RouterUtil.removeSchema(statementToString(replace), schemaInfo.getSchema());
        rrs.setStatement(sql);
        // try to route by ER parent partition key
        RouteResultset theRrs = routeByERParentColumn(rrs, tc, realVal, schemaInfo, service.getCharset().getClient());
        if (theRrs != null) {
            rrs.setFinishedRoute(true);
        } else {
            rrs.setFinishedExecute(true);
            fetchChildTableToRoute(tc, joinColumnVal, service, schema, sql, rrs, isExplain, tableName);
        }
    }

    private boolean isMultiReplace(SQLReplaceStatement insertStmt) {
        return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1);
    }

    /**
     * find joinColumn index
     *
     * @param schemaInfo  ManagerSchemaInfo
     * @param replaceStmt MySqlInsertStatement
     * @param joinColumn  joinColumn
     * @return -1 means no join key,otherwise means the index
     * @throws SQLNonTransientException if not find
     */
    private int getJoinColumnIndex(SchemaInfo schemaInfo, SQLReplaceStatement replaceStmt, String joinColumn) throws SQLNonTransientException {
        return tryGetShardingColIndex(schemaInfo, replaceStmt, joinColumn);
    }


    /**
     * insert into .... select .... OR insert into table() values (),(),....
     *
     * @param schemaInfo      ManagerSchemaInfo
     * @param rrs             RouteResultset
     * @param partitionColumn partitionColumn
     * @param replace         SQLReplaceStatement
     * @throws SQLNonTransientException if the column size of values is not correct
     */
    private void parserBatchInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                   SQLReplaceStatement replace, String clientCharset) throws SQLNonTransientException {
        // insert into table() values (),(),....
        SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        // column number
        int columnNum = getTableColumns(schemaInfo, replace.getColumns());
        int shardingColIndex = tryGetShardingColIndex(schemaInfo, replace, partitionColumn);
        String dataType = getShardingDataType(schemaInfo, partitionColumn);
        List<SQLInsertStatement.ValuesClause> valueClauseList = replace.getValuesList();
        Map<Integer, List<SQLInsertStatement.ValuesClause>> nodeValuesMap = new HashMap<>();
        ShardingTableConfig tableConfig = (ShardingTableConfig) (schema.getTables().get(tableName));
        for (SQLInsertStatement.ValuesClause valueClause : valueClauseList) {
            if (valueClause.getValues().size() != columnNum) {
                String msg = "bad insert sql columnSize != valueSize:" + columnNum + " != " + valueClause.getValues().size() + "values:" + valueClause;
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            SQLExpr expr = valueClause.getValues().get(shardingColIndex);
            String shardingValue = shardingValueToSting(expr, clientCharset, dataType);
            Integer nodeIndex = tableConfig.getFunction().calculate(shardingValue);
            // no part find for this record
            if (nodeIndex == null || nodeIndex >= tableConfig.getShardingNodes().size()) {
                String msg = "can't find any valid shardingnode :" + tableName + " -> " + partitionColumn + " -> " + shardingValue;
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            nodeValuesMap.computeIfAbsent(nodeIndex, k -> new ArrayList<>());
            nodeValuesMap.get(nodeIndex).add(valueClause);
        }

        RouteResultsetNode[] nodes = new RouteResultsetNode[nodeValuesMap.size()];
        int count = 0;
        for (Map.Entry<Integer, List<SQLInsertStatement.ValuesClause>> node : nodeValuesMap.entrySet()) {
            Integer nodeIndex = node.getKey();
            List<SQLInsertStatement.ValuesClause> valuesList = node.getValue();
            replace.getValuesList().clear();
            replace.getValuesList().addAll(valuesList);
            nodes[count] = new RouteResultsetNode(tableConfig.getShardingNodes().get(nodeIndex), rrs.getSqlType(),
                    RouterUtil.removeSchema(statementToString(replace), schemaInfo.getSchema()), Sets.newHashSet(schemaInfo.getSchema() + "." + schemaInfo.getTable()));
            count++;
        }
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }


    /**
     * insert single record
     *
     * @param schemaInfo       ManagerSchemaInfo
     * @param rrs              RouteResultset
     * @param partitionColumn  partitionColumn
     * @param replaceStatement SQLReplaceStatement
     * @throws SQLNonTransientException if not find a valid shardingNode
     */
    private void parserSingleInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                    SQLReplaceStatement replaceStatement, String clientCharset) throws SQLNonTransientException {
        int shardingColIndex = tryGetShardingColIndex(schemaInfo, replaceStatement, partitionColumn);
        SQLExpr valueExpr = replaceStatement.getValuesList().get(0).getValues().get(shardingColIndex);
        String dataType = getShardingDataType(schemaInfo, partitionColumn);
        String shardingValue = shardingValueToSting(valueExpr, clientCharset, dataType);
        ShardingTableConfig tableConfig = (ShardingTableConfig) (schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable()));
        Integer nodeIndex = tableConfig.getFunction().calculate(shardingValue);
        if (nodeIndex == null || nodeIndex >= tableConfig.getShardingNodes().size()) {
            String msg = "can't find any valid shardingNode :" + schemaInfo.getTable() + " -> " + partitionColumn + " -> " + shardingValue;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(tableConfig.getShardingNodes().get(nodeIndex),
                rrs.getSqlType(), RouterUtil.removeSchema(statementToString(replaceStatement), schemaInfo.getSchema()), Sets.newHashSet(schemaInfo.getSchema() + "." + schemaInfo.getTable()));

        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }


    @Override
    String getErrorMsg() {
        return "This `REPLACE ... SELECT Syntax` is not supported!";
    }

}
