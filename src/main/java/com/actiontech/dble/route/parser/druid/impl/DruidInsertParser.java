/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.config.model.sharding.table.ChildTableConfig;
import com.actiontech.dble.config.model.sharding.table.GlobalTableConfig;
import com.actiontech.dble.config.model.sharding.table.ShardingTableConfig;
import com.actiontech.dble.config.privileges.ShardingPrivileges;
import com.actiontech.dble.config.privileges.ShardingPrivileges.CheckType;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.singleton.SequenceManager;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

public class DruidInsertParser extends DruidInsertReplaceParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain)
            throws SQLException {

        MySqlInsertStatement insert = (MySqlInsertStatement) stmt;
        String schemaName = schema == null ? null : schema.getName();
        SQLExprTableSource tableSource = insert.getTableSource();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schemaName, tableSource);
        if (!ShardingPrivileges.checkPrivilege(service.getUserConfig(), schemaInfo.getSchema(), schemaInfo.getTable(), CheckType.INSERT)) {
            String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
            throw new SQLNonTransientException(msg);
        }

        if (insert.getQuery() != null) {
            tryRouteInsertQuery(service, rrs, stmt, visitor, schemaInfo);
            return schema;
        }

        if (insert.getValuesList().isEmpty()) {
            String msg = "Insert syntax error,no values in sql";
            throw new SQLNonTransientException(msg);
        }

        schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        if (parserNoSharding(service, schemaName, schemaInfo, rrs, insert)) {
            return schema;
        }

        BaseTableConfig tc = schema.getTables().get(tableName);
        checkTableExists(tc, schema.getName(), tableName, CheckType.INSERT);
        if (tc instanceof GlobalTableConfig) {
            String sql = rrs.getStatement();
            sql = RouterUtil.removeSchema(sql, schemaInfo.getSchema());
            rrs.setStatement(sql);
            RouterUtil.routeToMultiNode(false, rrs, tc.getShardingNodes(), true);
            rrs.setFinishedRoute(true);
            return schema;
        } else if (tc instanceof ChildTableConfig) { // insert childTable will finished router while parser
            ChildTableConfig child = (ChildTableConfig) tc;
            if (child.getIncrementColumn() != null) {
                insert = genNewMySqlInsertStatement(rrs, insert, schemaInfo, child.getIncrementColumn());
            }
            parserChildTable(schemaInfo, rrs, insert, service, isExplain);
            return schema;
        } else if (tc instanceof ShardingTableConfig) {
            ShardingTableConfig tableConfig = (ShardingTableConfig) tc;
            if (tableConfig.getIncrementColumn() != null) {
                insert = genNewMySqlInsertStatement(rrs, insert, schemaInfo, tableConfig.getIncrementColumn());
            }
            String partitionColumn = tableConfig.getShardingColumn();
            if (isMultiInsert(insert)) {
                parserBatchInsert(schemaInfo, rrs, partitionColumn, insert, service.getCharset().getClient());
            } else {
                parserSingleInsert(schemaInfo, rrs, partitionColumn, insert, service.getCharset().getClient());
            }
        } else {
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            ctx.addTable(new Pair<>(schema.getName(), tableName));
        }
        return schema;
    }

    private MySqlInsertStatement genNewMySqlInsertStatement(RouteResultset rrs, MySqlInsertStatement insert, SchemaInfo schemaInfo, String incrementColumn) throws SQLNonTransientException {
        SQLStatement stmt;
        String sql = changeSQLForIncrementColumn(schemaInfo, insert, rrs.getStatement(), incrementColumn);
        rrs.setStatement(sql);
        SQLStatementParser parser = new MySqlStatementParser(sql);
        stmt = parser.parseStatement();
        insert = (MySqlInsertStatement) stmt;
        return insert;
    }

    @Override
    SQLSelect acceptVisitor(SQLObject stmt, ServerSchemaStatVisitor visitor) {
        MySqlInsertStatement insert = (MySqlInsertStatement) stmt;
        insert.getQuery().accept(visitor);
        return insert.getQuery();
    }

    @Override
    int tryGetShardingColIndex(SchemaInfo schemaInfo, SQLStatement stmt, String partitionColumn) throws SQLNonTransientException {
        return tryGetShardingColIndex(schemaInfo, (MySqlInsertStatement) stmt, partitionColumn);
    }

    /**
     * find the index of the partition column
     *
     * @param schemaInfo      ManagerSchemaInfo
     * @param insertStmt      MySqlInsertStatement
     * @param partitionColumn partitionColumn
     * @return the index of the partition column
     * @throws SQLNonTransientException if not find
     */
    private int tryGetShardingColIndex(SchemaInfo schemaInfo, MySqlInsertStatement insertStmt, String partitionColumn)
            throws SQLNonTransientException {

        int shardingColIndex = getShardingColIndex(schemaInfo, insertStmt.getColumns(), partitionColumn);
        if (shardingColIndex != -1) return shardingColIndex;
        throw new SQLNonTransientException("bad insert sql, sharding column/joinKey:" + partitionColumn + " not provided," + insertStmt);
    }

    private boolean parserNoSharding(ShardingService service, String contextSchema, SchemaInfo schemaInfo, RouteResultset rrs,
                                     MySqlInsertStatement insert) throws SQLException {
        String noShardingNode = RouterUtil.isNoSharding(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
        if (noShardingNode != null) {
            StringPtr noShardingNodePr = new StringPtr(noShardingNode);
            Set<String> schemas = new HashSet<>();
            if (insert.getQuery() != null) {
                SQLSelectStatement selectStmt = new SQLSelectStatement(insert.getQuery());
                if (!SchemaUtil.isNoSharding(service, insert.getQuery().getQuery(), insert, selectStmt, contextSchema, schemas, noShardingNodePr)) {
                    return false;
                }
            }
            routeToNoSharding(schemaInfo.getSchemaConfig(), rrs, schemas, noShardingNodePr);
            return true;
        }
        return false;
    }

    /**
     * insert into ...values (),()... or insert into ...select.....
     *
     * @param insertStmt insertStmt
     * @return is Multi-Insert or not
     */
    private boolean isMultiInsert(MySqlInsertStatement insertStmt) {
        return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1);
    }

    private void parserChildTable(SchemaInfo schemaInfo, final RouteResultset rrs, MySqlInsertStatement insertStmt,
                                  final ShardingService service, boolean isExplain) throws SQLNonTransientException {

        final SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        final ChildTableConfig tc = (ChildTableConfig) (schema.getTables().get(tableName));
        if (isMultiInsert(insertStmt)) {
            String msg = "ChildTable multi insert not provided";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        String joinColumn = tc.getJoinColumn();
        int joinColumnIndex = getJoinColumnIndex(schemaInfo, insertStmt, joinColumn);
        final String joinColumnVal = insertStmt.getValues().getValues().get(joinColumnIndex).toString();
        String realVal = StringUtil.removeApostrophe(joinColumnVal);
        final String sql = RouterUtil.removeSchema(statementToString(insertStmt), schemaInfo.getSchema());
        rrs.setStatement(sql);
        // try to route by ER parent partion key
        RouteResultset theRrs = routeByERParentColumn(rrs, tc, realVal, schemaInfo, service.getCharset().getClient());
        if (theRrs != null) {
            rrs.setFinishedRoute(true);
        } else {
            rrs.setFinishedExecute(true);
            fetchChildTableToRoute(tc, joinColumnVal, service, schema, sql, rrs, isExplain);
        }
    }


    /**
     * @param schemaInfo      ManagerSchemaInfo
     * @param rrs             RouteResultset
     * @param partitionColumn partitionColumn
     * @param insertStmt      insertStmt
     * @throws SQLNonTransientException if not find an valid shardingNode
     */
    private void parserSingleInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                    MySqlInsertStatement insertStmt, String clientCharset) throws SQLNonTransientException {

        int shardingColIndex = tryGetShardingColIndex(schemaInfo, insertStmt, partitionColumn);
        SQLExpr valueExpr = insertStmt.getValues().getValues().get(shardingColIndex);
        TableMeta orgTbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(),
                schemaInfo.getTable());
        String shardingValue = shardingValueToSting(valueExpr, clientCharset, orgTbMeta.getColumns().get(shardingColIndex).getDataType());
        ShardingTableConfig tableConfig = (ShardingTableConfig) (schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable()));
        checkDefaultValues(shardingValue, tableConfig.getName(), schemaInfo.getSchema(), partitionColumn);
        Integer nodeIndex = tableConfig.getFunction().calculate(shardingValue);
        if (nodeIndex == null || nodeIndex >= tableConfig.getShardingNodes().size()) {
            String msg = "can't find any valid shardingNode :" + schemaInfo.getTable() + " -> " + partitionColumn + " -> " + shardingValue;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }

        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(tableConfig.getShardingNodes().get(nodeIndex), rrs.getSqlType(),
                RouterUtil.removeSchema(statementToString(insertStmt), schemaInfo.getSchema()));

        // insert into .... on duplicateKey
        //such as :INSERT INTO TABLEName (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE b=VALUES(b);
        //INSERT INTO TABLEName (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE c=c+1;
        if (insertStmt.getDuplicateKeyUpdate() != null) {
            List<SQLExpr> updateList = insertStmt.getDuplicateKeyUpdate();
            for (SQLExpr expr : updateList) {
                SQLBinaryOpExpr opExpr = (SQLBinaryOpExpr) expr;
                String column = StringUtil.removeBackQuote(opExpr.getLeft().toString().toUpperCase());
                if (column.equals(partitionColumn)) {
                    String msg = "Sharding column can't be updated: " + schemaInfo.getTable() + " -> " + partitionColumn;
                    LOGGER.info(msg);
                    throw new SQLNonTransientException(msg);
                }
            }
        }
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }

    /**
     * insert into .... select .... or insert into table() values (),(),....
     *
     * @param schemaInfo      ManagerSchemaInfo
     * @param rrs             RouteResultset
     * @param partitionColumn partitionColumn
     * @param insertStmt      insertStmt
     * @throws SQLNonTransientException if the column size of values is not correct
     */
    private void parserBatchInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                   MySqlInsertStatement insertStmt, String clientCharset) throws SQLNonTransientException {
        // insert into table() values (),(),....
        SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        // the size of columns
        int columnNum = getTableColumns(schemaInfo, insertStmt.getColumns());
        int shardingColIndex = tryGetShardingColIndex(schemaInfo, insertStmt, partitionColumn);
        List<ValuesClause> valueClauseList = insertStmt.getValuesList();
        Map<Integer, List<ValuesClause>> nodeValuesMap = new HashMap<>();
        ShardingTableConfig tableConfig = (ShardingTableConfig) (schema.getTables().get(tableName));
        for (ValuesClause valueClause : valueClauseList) {
            if (valueClause.getValues().size() != columnNum) {
                String msg = "bad insert sql columnSize != valueSize:" + columnNum + " != " + valueClause.getValues().size() + "values:" + valueClause;
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            SQLExpr expr = valueClause.getValues().get(shardingColIndex);
            TableMeta orgTbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(),
                    schemaInfo.getTable());
            String shardingValue = shardingValueToSting(expr, clientCharset, orgTbMeta.getColumns().get(shardingColIndex).getDataType());
            checkDefaultValues(shardingValue, tableConfig.getName(), schemaInfo.getSchema(), partitionColumn);
            Integer nodeIndex = tableConfig.getFunction().calculate(shardingValue);
            // null means can't find any valid index
            if (nodeIndex == null) {
                String msg = "can't find any valid shardingnode :" + tableName + " -> " + partitionColumn + " -> " + shardingValue;
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            nodeValuesMap.putIfAbsent(nodeIndex, new ArrayList<>());
            nodeValuesMap.get(nodeIndex).add(valueClause);
        }

        RouteResultsetNode[] nodes = new RouteResultsetNode[nodeValuesMap.size()];
        int count = 0;
        for (Map.Entry<Integer, List<ValuesClause>> node : nodeValuesMap.entrySet()) {
            Integer nodeIndex = node.getKey();
            List<ValuesClause> valuesList = node.getValue();
            insertStmt.getValuesList().clear();
            insertStmt.getValuesList().addAll(valuesList);
            nodes[count] = new RouteResultsetNode(tableConfig.getShardingNodes().get(nodeIndex), rrs.getSqlType(),
                    RouterUtil.removeSchema(statementToString(insertStmt), schemaInfo.getSchema()));
            count++;

        }
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }




    /**
     * find joinColumn index
     *
     * @param schemaInfo ManagerSchemaInfo
     * @param insertStmt MySqlInsertStatement
     * @param joinColumn    joinColumn
     * @return -1 means no join key,otherwise means the index
     * @throws SQLNonTransientException if not find
     */
    private int getJoinColumnIndex(SchemaInfo schemaInfo, MySqlInsertStatement insertStmt, String joinColumn) throws SQLNonTransientException {
        return tryGetShardingColIndex(schemaInfo, insertStmt, joinColumn);
    }

    private String changeSQLForIncrementColumn(SchemaInfo schemaInfo, MySqlInsertStatement insert, String originSql, String incrementColumn) throws SQLNonTransientException {

        TableMeta orgTbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
        if (orgTbMeta == null)
            return originSql;


        StringBuilder sb = new StringBuilder(200);
        sb.append("insert ");
        if (insert.isIgnore()) {
            sb.append("ignore ");
        }
        sb.append("into `");
        sb.append(schemaInfo.getTable());
        sb.append("`");

        List<SQLExpr> columns = insert.getColumns();

        int autoIncrement = -1;
        int colSize;
        // insert without columns :insert into t values(xxx,xxx)
        if (columns == null || columns.size() <= 0) {
            autoIncrement = getIncrementKeyIndex(schemaInfo, incrementColumn);
            colSize = orgTbMeta.getColumns().size();
        } else {
            genColumnNames(incrementColumn, sb, columns);
            colSize = columns.size();
            getIncrementKeyIndex(schemaInfo, incrementColumn);
            autoIncrement = columns.size();
            sb.append(",").append("`").append(incrementColumn).append("`");
            colSize++;
            sb.append(")");
        }

        sb.append(" values");
        String tableKey = StringUtil.getFullName(schemaInfo.getSchema(), schemaInfo.getTable());
        List<ValuesClause> vcl = insert.getValuesList();
        if (vcl != null && vcl.size() > 1) { // batch insert
            for (int j = 0; j < vcl.size(); j++) {
                if (j != vcl.size() - 1)
                    appendValues(tableKey, vcl.get(j).getValues(), sb, autoIncrement, colSize).append(",");
                else
                    appendValues(tableKey, vcl.get(j).getValues(), sb, autoIncrement, colSize);
            }
        } else {
            List<SQLExpr> values = insert.getValues().getValues();
            appendValues(tableKey, values, sb, autoIncrement, colSize);
        }

        List<SQLExpr> dku = insert.getDuplicateKeyUpdate();
        if (dku != null && dku.size() > 0) {
            genDuplicate(sb, dku);
        }
        return RouterUtil.removeSchema(sb.toString(), schemaInfo.getSchema());
    }

    private void genColumnNames(String incrementColumn, StringBuilder sb,
                                List<SQLExpr> columns) throws SQLNonTransientException {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).toString();
            if (i < columns.size() - 1) {
                sb.append("`").append(StringUtil.removeBackQuote(columnName)).append("`").append(",");
            } else {
                sb.append("`").append(StringUtil.removeBackQuote(columnName)).append("`");
            }
            String simpleColumnName = StringUtil.removeBackQuote(columnName);
            if (simpleColumnName.equalsIgnoreCase(incrementColumn)) {
                String msg = "In insert Syntax, you can't set value for Autoincrement column!";
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
        }
    }

    private void genDuplicate(StringBuilder sb, List<SQLExpr> dku) throws SQLNonTransientException {
        sb.append(" on duplicate key update ");
        for (int i = 0; i < dku.size(); i++) {
            SQLExpr exp = dku.get(i);
            if (!(exp instanceof SQLBinaryOpExpr)) {
                String msg = "not supported! on duplicate key update exp is " + exp.getClass();
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) exp;
            sb.append(binaryOpExpr.toString());
            if (i < dku.size() - 1) {
                sb.append(",");
            }
        }
    }


    private static StringBuilder appendValues(String tableKey, List<SQLExpr> values, StringBuilder sb, int autoIncrement,
                                              int colSize) throws SQLNonTransientException {

        int size = values.size();
        int checkSize = colSize - (autoIncrement < 0 ? 0 : 1);
        if (checkSize < size) {
            String msg = "In insert Syntax, you can't set value for Autoincrement column! Or column count doesn't match value count";
            if (autoIncrement < 0) {
                msg = "In insert Syntax, you can't set value for Global check column! Or column count doesn't match value count";
            }
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        } else if (checkSize > size) {
            String msg = "Column count doesn't match value count";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        sb.append("(");
        int iValue = 0;
        for (int i = 0; i < colSize; i++) {
            if (i == autoIncrement) {
                long id = SequenceManager.getHandler().nextId(tableKey);
                sb.append(id);
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


}
