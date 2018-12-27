/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.FetchStoreNodeOfChildTableHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerPrivileges;
import com.actiontech.dble.config.ServerPrivileges.CheckType;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

public class DruidInsertParser extends DruidInsertReplaceParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc)
            throws SQLException {

        MySqlInsertStatement insert = (MySqlInsertStatement) stmt;
        String schemaName = schema == null ? null : schema.getName();
        SQLExprTableSource tableSource = insert.getTableSource();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, tableSource);
        if (!ServerPrivileges.checkPrivilege(sc, schemaInfo.getSchema(), schemaInfo.getTable(), CheckType.INSERT)) {
            String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
            throw new SQLNonTransientException(msg);
        }

        schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        if (parserNoSharding(sc, schemaName, schemaInfo, rrs, insert)) {
            return schema;
        }

        if (insert.getQuery() != null) {
            // insert into .... select ....
            String msg = "`INSERT ... SELECT Syntax` is not supported!";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }

        TableConfig tc = schema.getTables().get(tableName);
        checkTableExists(tc, schema.getName(), tableName, CheckType.INSERT);
        if (tc.isGlobalTable()) {
            String sql = rrs.getStatement();
            if (tc.isAutoIncrement() || GlobalTableUtil.useGlobalTableCheck()) {
                sql = convertInsertSQL(schemaInfo, insert, sql, tc, GlobalTableUtil.useGlobalTableCheck());
            } else {
                sql = RouterUtil.removeSchema(sql, schemaInfo.getSchema());
            }
            rrs.setStatement(sql);
            RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), tc.isGlobalTable());
            rrs.setFinishedRoute(true);
            return schema;
        }

        if (tc.isAutoIncrement()) {
            String sql = convertInsertSQL(schemaInfo, insert, rrs.getStatement(), tc, false);
            rrs.setStatement(sql);
            SQLStatementParser parser = new MySqlStatementParser(sql);
            stmt = parser.parseStatement();
            insert = (MySqlInsertStatement) stmt;
        }
        // insert childTable will finished router while parser
        if (tc.getParentTC() != null) {
            parserChildTable(schemaInfo, rrs, insert, sc);
            return schema;
        }
        String partitionColumn = tc.getPartitionColumn();
        if (partitionColumn != null) {
            if (isMultiInsert(insert)) {
                parserBatchInsert(schemaInfo, rrs, partitionColumn, insert);
            } else {
                parserSingleInsert(schemaInfo, rrs, partitionColumn, insert);
            }
        } else {
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            ctx.addTable(tableName);
        }
        return schema;
    }

    private boolean parserNoSharding(ServerConnection sc, String contextSchema, SchemaInfo schemaInfo, RouteResultset rrs,
                                     MySqlInsertStatement insert) throws SQLException {
        String noShardingNode = RouterUtil.isNoSharding(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
        if (noShardingNode != null) {
            StringPtr noShardingNodePr = new StringPtr(noShardingNode);
            Set<String> schemas = new HashSet<>();
            if (insert.getQuery() != null) {
                SQLSelectStatement selectStmt = new SQLSelectStatement(insert.getQuery());
                if (!SchemaUtil.isNoSharding(sc, insert.getQuery().getQuery(), insert, selectStmt, contextSchema, schemas, noShardingNodePr)) {
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
                                  final ServerConnection sc) throws SQLNonTransientException {

        final SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        final TableConfig tc = schema.getTables().get(tableName);
        if (isMultiInsert(insertStmt)) {
            String msg = "ChildTable multi insert not provided";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        String joinKey = tc.getJoinKey();
        int joinKeyIndex = getJoinKeyIndex(schemaInfo, insertStmt, joinKey);
        final String joinKeyVal = insertStmt.getValues().getValues().get(joinKeyIndex).toString();
        String realVal = StringUtil.removeApostrophe(joinKeyVal);
        final String sql = RouterUtil.removeSchema(statementToString(insertStmt), schemaInfo.getSchema());
        rrs.setStatement(sql);
        // try to route by ER parent partion key
        RouteResultset theRrs = routeByERParentKey(rrs, tc, realVal, schemaInfo);
        if (theRrs != null) {
            rrs.setFinishedRoute(true);
        } else {
            rrs.setFinishedExecute(true);
            DbleServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
                //get child result will be blocked, so use ComplexQueryExecutor
                @Override
                public void run() {
                    // route by sql query root parent's data node
                    String findRootTBSql = tc.getLocateRTableKeySql() + joinKeyVal;
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("to find root parent's node sql :" + findRootTBSql);
                    }
                    FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler(findRootTBSql, sc.getSession2());
                    String dn = fetchHandler.execute(schema.getName(), tc.getRootParent().getDataNodes());
                    if (dn == null) {
                        sc.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "can't find (root) parent sharding node for sql:" + sql);
                        return;
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("found partition node for child table to insert " + dn + " sql :" + sql);
                    }
                    RouterUtil.routeToSingleNode(rrs, dn);
                    sc.getSession2().execute(rrs);
                }
            });
        }
    }


    /**
     * @param schemaInfo SchemaInfo
     * @param rrs RouteResultset
     * @param partitionColumn partitionColumn
     * @param insertStmt insertStmt
     * @throws SQLNonTransientException if not find an valid data node
     */
    private void parserSingleInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                    MySqlInsertStatement insertStmt) throws SQLNonTransientException {

        int shardingColIndex = tryGetShardingColIndex(schemaInfo, insertStmt, partitionColumn);
        SQLExpr valueExpr = insertStmt.getValues().getValues().get(shardingColIndex);
        String shardingValue = shardingValueToSting(valueExpr);
        TableConfig tableConfig = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
        AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
        checkDefaultValues(shardingValue, tableConfig, schemaInfo.getSchema(), partitionColumn);
        Integer nodeIndex = algorithm.calculate(shardingValue);
        if (nodeIndex == null || nodeIndex >= tableConfig.getDataNodes().size()) {
            String msg = "can't find any valid data node :" + schemaInfo.getTable() + " -> " + partitionColumn + " -> " + shardingValue;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }

        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex), rrs.getSqlType(),
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
     * @param schemaInfo SchemaInfo
     * @param rrs RouteResultset
     * @param partitionColumn partitionColumn
     * @param insertStmt insertStmt
     * @throws SQLNonTransientException if the column size of values is not correct
     */
    private void parserBatchInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                   MySqlInsertStatement insertStmt) throws SQLNonTransientException {
        // insert into table() values (),(),....
        SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        // the size of columns
        int columnNum = getTableColumns(schemaInfo, insertStmt.getColumns());
        int shardingColIndex = tryGetShardingColIndex(schemaInfo, insertStmt, partitionColumn);
        List<ValuesClause> valueClauseList = insertStmt.getValuesList();
        Map<Integer, List<ValuesClause>> nodeValuesMap = new HashMap<>();
        TableConfig tableConfig = schema.getTables().get(tableName);
        AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
        for (ValuesClause valueClause : valueClauseList) {
            if (valueClause.getValues().size() != columnNum) {
                String msg = "bad insert sql columnSize != valueSize:" + columnNum + " != " + valueClause.getValues().size() + "values:" + valueClause;
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            SQLExpr expr = valueClause.getValues().get(shardingColIndex);
            String shardingValue = shardingValueToSting(expr);
            checkDefaultValues(shardingValue, tableConfig, schemaInfo.getSchema(), partitionColumn);
            Integer nodeIndex = algorithm.calculate(shardingValue);
            // null means can't find any valid index
            if (nodeIndex == null) {
                String msg = "can't find any valid datanode :" + tableName + " -> " + partitionColumn + " -> " + shardingValue;
                LOGGER.info(msg);
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
            insertStmt.getValuesList().clear();
            insertStmt.getValuesList().addAll(valuesList);
            nodes[count] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex), rrs.getSqlType(),
                    RouterUtil.removeSchema(statementToString(insertStmt), schemaInfo.getSchema()));
            count++;

        }
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }

    /**
     * find the index of the partition column
     * @param schemaInfo SchemaInfo
     * @param insertStmt MySqlInsertStatement
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


    /**
     * find joinKey index
     *
     * @param schemaInfo SchemaInfo
     * @param insertStmt MySqlInsertStatement
     * @param joinKey joinKey
     * @return -1 means no join key,otherwise means the index
     * @throws SQLNonTransientException if not find
     */
    private int getJoinKeyIndex(SchemaInfo schemaInfo, MySqlInsertStatement insertStmt, String joinKey) throws SQLNonTransientException {
        return tryGetShardingColIndex(schemaInfo, insertStmt, joinKey);
    }

    private String convertInsertSQL(SchemaInfo schemaInfo, MySqlInsertStatement insert, String originSql, TableConfig tc,
                                    boolean isGlobalCheck) throws SQLNonTransientException {

        StructureMeta.TableMeta orgTbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
        if (orgTbMeta == null)
            return originSql;

        boolean isAutoIncrement = tc.isAutoIncrement();

        if (isGlobalCheck && !GlobalTableUtil.isInnerColExist(schemaInfo, orgTbMeta)) {
            if (!isAutoIncrement) {
                return originSql;
            } else {
                isGlobalCheck = false;
            }
        }

        StringBuilder sb = new StringBuilder(200);
        sb.append("insert ");
        if (insert.isIgnore()) {
            sb.append("ignore ");
        }
        sb.append("into ");
        sb.append(schemaInfo.getTable());

        List<SQLExpr> columns = insert.getColumns();

        int autoIncrement = -1;
        int idxGlobal = -1;
        int colSize;
        // insert without columns :insert into t values(xxx,xxx)
        if (columns == null || columns.size() <= 0) {
            if (isAutoIncrement) {
                autoIncrement = getPrimaryKeyIndex(schemaInfo, tc.getPrimaryKey());
            }
            colSize = orgTbMeta.getColumnsList().size();
            idxGlobal = getIdxGlobalByMeta(isGlobalCheck, orgTbMeta, sb, colSize);
        } else {
            genColumnNames(tc, isGlobalCheck, isAutoIncrement, sb, columns);
            colSize = columns.size();
            if (isAutoIncrement) {
                autoIncrement = columns.size();
                sb.append(",").append(tc.getPrimaryKey());
                colSize++;
            }
            if (isGlobalCheck) {
                idxGlobal = isAutoIncrement ? columns.size() + 1 : columns.size();
                sb.append(",").append(GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN);
                colSize++;
            }
            sb.append(")");
        }

        sb.append(" values");
        String tableKey = StringUtil.getFullName(schemaInfo.getSchema(), schemaInfo.getTable());
        List<ValuesClause> vcl = insert.getValuesList();
        if (vcl != null && vcl.size() > 1) { // batch insert
            for (int j = 0; j < vcl.size(); j++) {
                if (j != vcl.size() - 1)
                    appendValues(tableKey, vcl.get(j).getValues(), sb, autoIncrement, idxGlobal, colSize).append(",");
                else
                    appendValues(tableKey, vcl.get(j).getValues(), sb, autoIncrement, idxGlobal, colSize);
            }
        } else {
            List<SQLExpr> values = insert.getValues().getValues();
            appendValues(tableKey, values, sb, autoIncrement, idxGlobal, colSize);
        }

        List<SQLExpr> dku = insert.getDuplicateKeyUpdate();
        if (dku != null && dku.size() > 0) {
            genDuplicate(isGlobalCheck, sb, dku);
        }
        return RouterUtil.removeSchema(sb.toString(), schemaInfo.getSchema());
    }

    private void genColumnNames(TableConfig tc, boolean isGlobalCheck, boolean isAutoIncrement, StringBuilder sb,
                                List<SQLExpr> columns) throws SQLNonTransientException {

        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).toString();
            if (i < columns.size() - 1) {
                sb.append(columnName).append(",");
            } else {
                sb.append(columnName);
            }
            String simpleColumnName = StringUtil.removeBackQuote(columnName);
            if (isGlobalCheck && simpleColumnName.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN)) {
                String msg = "In insert Syntax, you can't set value for Global check column!";
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            if (isAutoIncrement && simpleColumnName.equalsIgnoreCase(tc.getPrimaryKey())) {
                String msg = "In insert Syntax, you can't set value for Autoincrement column!";
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
        }
    }

    private void genDuplicate(boolean isGlobalCheck, StringBuilder sb, List<SQLExpr> dku) throws SQLNonTransientException {
        boolean flag = false;
        sb.append(" on duplicate key update ");
        for (int i = 0; i < dku.size(); i++) {
            SQLExpr exp = dku.get(i);
            if (!(exp instanceof SQLBinaryOpExpr)) {
                String msg = "not supported! on duplicate key update exp is " + exp.getClass();
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) exp;
            if (isGlobalCheck && !flag && GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN.equals(binaryOpExpr.getLeft().toString())) {
                flag = true;
                onDuplicateGlobalColumn(sb);
            } else {
                sb.append(binaryOpExpr.toString());
            }
            if (i < dku.size() - 1) {
                sb.append(",");
            }
        }
        if (isGlobalCheck && !flag) {
            sb.append(",");
            onDuplicateGlobalColumn(sb);
        }
    }

    private static void onDuplicateGlobalColumn(StringBuilder sb) {
        sb.append(GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN);
        sb.append("=values(");
        sb.append(GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN);
        sb.append(")");
    }

    private static StringBuilder appendValues(String tableKey, List<SQLExpr> values, StringBuilder sb, int autoIncrement, int idxGlobal,
                                              int colSize) throws SQLNonTransientException {

        int size = values.size();
        int checkSize = colSize - (autoIncrement < 0 ? 0 : 1) - (idxGlobal < 0 ? 0 : 1);
        if (checkSize != size) {
            String msg = "In insert Syntax, you can't set value for Autoincrement column!";
            if (autoIncrement < 0) {
                msg = "In insert Syntax, you can't set value for Global check column!";
            }
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        sb.append("(");
        int iValue = 0;
        for (int i = 0; i < colSize; i++) {
            if (i == idxGlobal) {
                sb.append(String.valueOf(new Date().getTime()));
            } else if (i == autoIncrement) {
                long id = DbleServer.getInstance().getSequenceHandler().nextId(tableKey);
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
