/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.FetchStoreNodeOfChildTableHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerPrivileges;
import com.actiontech.dble.config.ServerPrivileges.Checktype;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.ReplaceInsertUtil;
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

public class DruidInsertParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc)
            throws SQLException {
        MySqlInsertStatement insert = (MySqlInsertStatement) stmt;
        String schemaName = schema == null ? null : schema.getName();
        SQLExprTableSource tableSource = insert.getTableSource();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, tableSource);
        if (!ServerPrivileges.checkPrivilege(sc, schemaInfo.getSchema(), schemaInfo.getTable(), Checktype.INSERT)) {
            String msg = "The statement DML privilege check is not passed, sql:" + stmt;
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
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        TableConfig tc = schema.getTables().get(tableName);
        if (tc == null) {
            String msg = "Table '" + schema.getName() + "." + tableName + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }
        if (tc.isGlobalTable()) {
            String sql = rrs.getStatement();
            if (tc.isAutoIncrement() || GlobalTableUtil.useGlobleTableCheck()) {
                sql = convertInsertSQL(schemaInfo, insert, sql, tc, GlobalTableUtil.useGlobleTableCheck());
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

    private boolean parserNoSharding(ServerConnection sc, String contextSchema, SchemaInfo schemaInfo, RouteResultset rrs, MySqlInsertStatement insert) throws SQLException {
        if (RouterUtil.isNoSharding(schemaInfo.getSchemaConfig(), schemaInfo.getTable())) {
            if (insert.getQuery() != null) {
                SQLSelectStatement selectStmt = new SQLSelectStatement(insert.getQuery());
                StringPtr sqlSchema = new StringPtr(schemaInfo.getSchema());
                if (!SchemaUtil.isNoSharding(sc, insert.getQuery().getQuery(), selectStmt, contextSchema, sqlSchema)) {
                    return false;
                }
            }
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            RouterUtil.routeToSingleNode(rrs, schemaInfo.getSchemaConfig().getDataNode());
            return true;
        }
        return false;
    }

    /**
     * insert into ...values (),()... or insert into ...select.....
     *
     * @param insertStmt
     * @return
     */
    private boolean isMultiInsert(MySqlInsertStatement insertStmt) {
        return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1);
    }

    private RouteResultset parserChildTable(SchemaInfo schemaInfo, RouteResultset rrs, MySqlInsertStatement insertStmt, ServerConnection sc) throws SQLNonTransientException {
        SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        TableConfig tc = schema.getTables().get(tableName);
        if (isMultiInsert(insertStmt)) {
            String msg = "ChildTable multi insert not provided";
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        String joinKey = tc.getJoinKey();
        int joinKeyIndex = getJoinKeyIndex(schemaInfo, insertStmt, joinKey);
        String joinKeyVal = insertStmt.getValues().getValues().get(joinKeyIndex).toString();
        String realVal = StringUtil.removeApostrophe(joinKeyVal);
        String sql = RouterUtil.removeSchema(insertStmt.toString(), schemaInfo.getSchema());
        rrs.setStatement(sql);
        // try to route by ER parent partion key
        RouteResultset theRrs = ReplaceInsertUtil.routeByERParentKey(rrs, tc, realVal);
        if (theRrs != null) {
            rrs.setFinishedRoute(true);
            return theRrs;
        }
        // route by sql query root parent's datanode
        String findRootTBSql = tc.getLocateRTableKeySql().toLowerCase() + joinKeyVal;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("find root parent's node sql " + findRootTBSql);
        }
        FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler(findRootTBSql, sc.getSession2());
        String dn = fetchHandler.execute(schema.getName(), tc.getRootParent().getDataNodes());
        if (dn == null) {
            throw new SQLNonTransientException("can't find (root) parent sharding node for sql:" + sql);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("found partion node for child table to insert " + dn + " sql :" + sql);
        }
        return RouterUtil.routeToSingleNode(rrs, dn);
    }


    /**
     * @param schemaInfo
     * @param rrs
     * @param partitionColumn
     * @param insertStmt
     * @throws SQLNonTransientException
     */
    private void parserSingleInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn, MySqlInsertStatement insertStmt) throws SQLNonTransientException {
        int shardingColIndex = getShardingColIndex(schemaInfo, insertStmt, partitionColumn);
        SQLExpr valueExpr = insertStmt.getValues().getValues().get(shardingColIndex);
        String shardingValue = ReplaceInsertUtil.shardingValueToSting(valueExpr);
        TableConfig tableConfig = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
        AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
        Integer nodeIndex = algorithm.calculate(shardingValue);
        if (nodeIndex == null) {
            String msg = "can't find any valid datanode :" + schemaInfo.getTable() + " -> " +
                    partitionColumn + " -> " + shardingValue;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex), rrs.getSqlType(), RouterUtil.removeSchema(insertStmt.toString(), schemaInfo.getSchema()));

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
                    LOGGER.warn(msg);
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
     * @param schemaInfo
     * @param rrs
     * @param partitionColumn
     * @param insertStmt
     * @throws SQLNonTransientException
     */
    private void parserBatchInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                   MySqlInsertStatement insertStmt) throws SQLNonTransientException {
        // insert into table() values (),(),....
        SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        // the size of columns
        int columnNum = getTableColumns(schemaInfo, insertStmt);
        int shardingColIndex = getShardingColIndex(schemaInfo, insertStmt, partitionColumn);
        List<ValuesClause> valueClauseList = insertStmt.getValuesList();
        Map<Integer, List<ValuesClause>> nodeValuesMap = new HashMap<>();
        TableConfig tableConfig = schema.getTables().get(tableName);
        AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
        for (ValuesClause valueClause : valueClauseList) {
            if (valueClause.getValues().size() != columnNum) {
                String msg = "bad insert sql columnSize != valueSize:" + columnNum + " != " +
                        valueClause.getValues().size() + "values:" + valueClause;
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            SQLExpr expr = valueClause.getValues().get(shardingColIndex);
            String shardingValue = ReplaceInsertUtil.shardingValueToSting(expr);
            Integer nodeIndex = algorithm.calculate(shardingValue);
            // null means can't find any valid index
            if (nodeIndex == null) {
                String msg = "can't find any valid datanode :" + tableName + " -> " + partitionColumn + " -> " + shardingValue;
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
                    RouterUtil.removeSchema(insertStmt.toString(), schemaInfo.getSchema()));
            count++;

        }
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }

    private int getPrimaryKeyIndex(SchemaInfo schemaInfo, String primaryKeyColumn) throws SQLNonTransientException {
        if (primaryKeyColumn == null) {
            throw new SQLNonTransientException("please make sure the primaryKey's config is not null in schemal.xml");
        }
        int primaryKeyIndex = -1;
        StructureMeta.TableMeta tbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(),
                schemaInfo.getTable());
        if (tbMeta != null) {
            boolean hasPrimaryKey = false;
            StructureMeta.IndexMeta primaryKey = tbMeta.getPrimary();
            if (primaryKey != null) {
                for (int i = 0; i < tbMeta.getPrimary().getColumnsCount(); i++) {
                    if (primaryKeyColumn.equalsIgnoreCase(tbMeta.getPrimary().getColumns(i))) {
                        hasPrimaryKey = true;
                        break;
                    }
                }
            }
            if (!hasPrimaryKey) {
                String msg = "please make sure your table structure has primaryKey";
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }

            for (int i = 0; i < tbMeta.getColumnsCount(); i++) {
                if (primaryKeyColumn.equalsIgnoreCase(tbMeta.getColumns(i).getName())) {
                    return i;
                }
            }
        }
        return primaryKeyIndex;
    }

    /**
     * find the index of the partition column
     *
     * @param insertStmt
     * @param partitionColumn
     * @return
     * @throws SQLNonTransientException
     */
    private int getShardingColIndex(SchemaInfo schemaInfo, MySqlInsertStatement insertStmt, String partitionColumn) throws SQLNonTransientException {
        int shardingColIndex = -1;
        if (insertStmt.getColumns() == null || insertStmt.getColumns().size() == 0) {
            StructureMeta.TableMeta tbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
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
            if (partitionColumn.equalsIgnoreCase(StringUtil.removeBackQuote(insertStmt.getColumns().get(i).toString()))) {
                return i;
            }
        }
        String msg = "bad insert sql, sharding column/joinKey:" + partitionColumn + " not provided," + insertStmt;
        LOGGER.warn(msg);
        throw new SQLNonTransientException(msg);
    }

    private int getTableColumns(SchemaInfo schemaInfo, MySqlInsertStatement insertStmt)
            throws SQLNonTransientException {
        if (insertStmt.getColumns() == null || insertStmt.getColumns().size() == 0) {
            StructureMeta.TableMeta tbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
            if (tbMeta == null) {
                String msg = "Meta data of table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist";
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            return tbMeta.getColumnsCount();
        } else {
            return insertStmt.getColumns().size();
        }
    }

    /**
     * find joinKey index
     *
     * @param schemaInfo
     * @param insertStmt
     * @param joinKey
     * @return -1 means no join key,otherwise means the index
     * @throws SQLNonTransientException
     */
    private int getJoinKeyIndex(SchemaInfo schemaInfo, MySqlInsertStatement insertStmt, String joinKey) throws SQLNonTransientException {
        return getShardingColIndex(schemaInfo, insertStmt, joinKey);
    }

    private String convertInsertSQL(SchemaInfo schemaInfo, MySqlInsertStatement insert, String originSql, TableConfig tc, boolean isGlobalCheck) throws SQLNonTransientException {
        StructureMeta.TableMeta orgTbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(),
                schemaInfo.getTable());
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
            idxGlobal = ReplaceInsertUtil.getIdxGlobalByMeta(isGlobalCheck, orgTbMeta, sb, colSize);
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
            List<SQLExpr> valuse = insert.getValues().getValues();
            appendValues(tableKey, valuse, sb, autoIncrement, idxGlobal, colSize);
        }

        List<SQLExpr> dku = insert.getDuplicateKeyUpdate();
        if (dku != null && dku.size() > 0) {
            genDuplicate(isGlobalCheck, sb, dku);
        }
        return RouterUtil.removeSchema(sb.toString(), schemaInfo.getSchema());
    }

    private void genColumnNames(TableConfig tc, boolean isGlobalCheck, boolean isAutoIncrement, StringBuilder sb, List<SQLExpr> columns) throws SQLNonTransientException {
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
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            if (isAutoIncrement && simpleColumnName.equalsIgnoreCase(tc.getPrimaryKey())) {
                String msg = "In insert Syntax, you can't set value for Autoincrement column!";
                LOGGER.warn(msg);
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
                LOGGER.warn(msg);
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

    private static StringBuilder appendValues(String tableKey, List<SQLExpr> valuse, StringBuilder sb, int autoIncrement, int idxGlobal, int colSize) throws SQLNonTransientException {
        int size = valuse.size();
        int checkSize = colSize - (autoIncrement < 0 ? 0 : 1) - (idxGlobal < 0 ? 0 : 1);
        if (checkSize != size) {
            String msg = "In insert Syntax, you can't set value for Autoincrement column!";
            if (autoIncrement < 0) {
                msg = "In insert Syntax, you can't set value for Global check column!";
            }
            LOGGER.warn(msg);
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
                String value = SQLUtils.toMySqlString(valuse.get(iValue++));
                sb.append(value);
            }
            if (i < colSize - 1) {
                sb.append(",");
            }
        }
        return sb.append(")");
    }

}
