/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.FetchStoreNodeOfChildTableHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerPrivileges;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.route.parser.druid.ReplaceTemp;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.ReplaceInsertUtil;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * Created by szf on 2017/8/18.
 */
public class DruidReplaceParser extends DefaultDruidParser {

    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc)
            throws SQLException {
        //data & object prepare
        MySqlReplaceStatement replace = (MySqlReplaceStatement) stmt;
        String schemaName = schema == null ? null : schema.getName();
        SQLExprTableSource tableSource = replace.getTableSource();
        SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, tableSource);

        //privilege check
        if (!ServerPrivileges.checkPrivilege(sc, schemaInfo.getSchema(), schemaInfo.getTable(), ServerPrivileges.Checktype.INSERT)) {
            String msg = "The statement DML privilege check is not passed, sql:" + stmt;
            throw new SQLNonTransientException(msg);
        }

        //No sharding table check
        schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        if (parserNoSharding(sc, schemaName, schemaInfo, rrs, replace)) {
            return schema;
        }
        if (replace.getQuery() != null) {
            //replace into ...select with sharding not supported
            String msg = "`INSERT ... SELECT Syntax` is not supported!";
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }

        //check the config of target table
        TableConfig tc = schema.getTables().get(tableName);
        if (tc == null) {
            String msg = "Table '" + schema.getName() + "." + tableName + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }

        //if the target table is global table than
        if (tc.isGlobalTable()) {
            String sql = rrs.getStatement();
            if (tc.isAutoIncrement() || GlobalTableUtil.useGlobleTableCheck()) {
                sql = convertReplaceSQL(schemaInfo, replace, sql, tc, GlobalTableUtil.useGlobleTableCheck(), sc);
            } else {
                sql = RouterUtil.removeSchema(sql, schemaInfo.getSchema());
            }
            rrs.setStatement(sql);
            RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), tc.isGlobalTable());
            rrs.setFinishedRoute(true);
            return schema;
        }

        if (tc.isAutoIncrement()) {
            String sql = convertReplaceSQL(schemaInfo, replace, rrs.getStatement(), tc, false, sc);
            rrs.setStatement(sql);
            SQLStatementParser parser = new MySqlStatementParser(sql);
            stmt = parser.parseStatement();
            replace = (MySqlReplaceStatement) stmt;
        }

        // childTable can be route in this part
        if (tc.getParentTC() != null) {
            parserChildTable(schemaInfo, rrs, replace, sc);
            return schema;
        }

        String partitionColumn = tc.getPartitionColumn();
        if (partitionColumn != null) {
            if (isMultiReplace(replace)) {
                parserBatchInsert(schemaInfo, rrs, partitionColumn, replace);
            } else {
                parserSingleInsert(schemaInfo, rrs, partitionColumn, replace);
            }
        } else {
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            ctx.addTable(tableName);
        }

        return schema;
    }


    /**
     * check if the nosharding tables are Involved
     *
     * @param sc
     * @param contextSchema
     * @param schemaInfo
     * @param rrs
     * @param replace
     * @return
     * @throws SQLException
     */
    private boolean parserNoSharding(ServerConnection sc, String contextSchema, SchemaUtil.SchemaInfo schemaInfo, RouteResultset rrs, MySqlReplaceStatement replace) throws SQLException {
        if (RouterUtil.isNoSharding(schemaInfo.getSchemaConfig(), schemaInfo.getTable())) {
            if (replace.getQuery() != null) {
                StringPtr sqlSchema = new StringPtr(schemaInfo.getSchema());
                //replace into ...select  if the both table is nosharding table
                if (!SchemaUtil.isNoSharding(sc, replace.getQuery(), contextSchema, sqlSchema)) {
                    return false;
                }
            }
            rrs.setStatement(RouterUtil.removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
            RouterUtil.routeToSingleNode(rrs, schemaInfo.getSchemaConfig().getDataNode());
            return true;
        }
        return false;
    }


    private String convertReplaceSQL(SchemaUtil.SchemaInfo schemaInfo, MySqlReplaceStatement replace, String originSql, TableConfig tc, boolean isGlobalCheck, ServerConnection sc) throws SQLNonTransientException {
        StructureMeta.TableMeta orgTbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(),
                schemaInfo.getTable());
        if (orgTbMeta == null)
            return originSql;

        boolean isAutoIncrement = tc.isAutoIncrement();

        String tableName = schemaInfo.getTable();
        if (isGlobalCheck && !GlobalTableUtil.isInnerColExist(schemaInfo, orgTbMeta)) {
            if (!isAutoIncrement) {
                return originSql;
            } else {
                isGlobalCheck = false;
            }
        }

        StringBuilder sb = new StringBuilder(200/* this is to improve the performance) */).append("replace into ").append(tableName);

        List<SQLExpr> columns = replace.getColumns();

        int autoIncrement = -1;
        int idxGlobal = -1;
        int colSize;
        // replace with no column name ：replace into t values(xxx,xxx)
        if (columns == null || columns.size() <= 0) {
            if (isAutoIncrement) {
                autoIncrement = getPrimaryKeyIndex(schemaInfo, tc.getPrimaryKey());
            }
            colSize = orgTbMeta.getColumnsList().size();
            idxGlobal = ReplaceInsertUtil.getIdxGlobalByMeta(isGlobalCheck, orgTbMeta, sb, colSize);
        } else { // replace sql with  column names
            boolean hasPkInSql = concatColumns(replace, tc, isGlobalCheck, isAutoIncrement, sb, columns);
            colSize = columns.size();
            if (isAutoIncrement && !hasPkInSql) {
                autoIncrement = columns.size();
                sb.append(",").append(tc.getPrimaryKey());
                colSize++;
            }
            if (isGlobalCheck) {
                idxGlobal = (isAutoIncrement && !hasPkInSql) ? columns.size() + 1 : columns.size();
                sb.append(",").append(GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN);
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
                    appendValues(tableKey, vcl.get(j).getValues(), sb, autoIncrement, idxGlobal, colSize).append(",");
                else
                    appendValues(tableKey, vcl.get(j).getValues(), sb, autoIncrement, idxGlobal, colSize);
            }
        } else { // single line insert
            List<SQLExpr> valuse = replace.getValuesList().get(0).getValues();
            appendValues(tableKey, valuse, sb, autoIncrement, idxGlobal, colSize);
        }

        return RouterUtil.removeSchema(sb.toString(), schemaInfo.getSchema());
    }

    private boolean concatColumns(MySqlReplaceStatement replace, TableConfig tc, boolean isGlobalCheck, boolean isAutoIncrement, StringBuilder sb, List<SQLExpr> columns) throws SQLNonTransientException {
        sb.append("(");
        boolean hasPkInSql = false;
        for (int i = 0; i < columns.size(); i++) {
            if (isAutoIncrement && columns.get(i).toString().equalsIgnoreCase(tc.getPrimaryKey())) {
                hasPkInSql = true;
            }
            if (i < columns.size() - 1)
                sb.append(columns.get(i).toString()).append(",");
            else
                sb.append(columns.get(i).toString());
            String column = StringUtil.removeBackQuote(replace.getColumns().get(i).toString());
            if (isGlobalCheck && column.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN)) {
                String msg = "In insert Syntax, you can't set value for Global check column!";
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }
        }
        return hasPkInSql;
    }


    private int getPrimaryKeyIndex(SchemaUtil.SchemaInfo schemaInfo, String primaryKeyColumn) throws SQLNonTransientException {
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
     * because of the replace can use a
     *
     * @param tableKey
     * @param valuse
     * @param sb
     * @param autoIncrement
     * @param idxGlobal
     * @param colSize
     * @return
     * @throws SQLNonTransientException
     */
    private static StringBuilder appendValues(String tableKey, List<SQLExpr> valuse, StringBuilder sb,
                                              int autoIncrement, int idxGlobal, int colSize) throws SQLNonTransientException {
        // check the value number & the column number is all right
        int size = valuse.size();
        int checkSize = colSize - (idxGlobal < 0 ? 0 : 1);
        if (checkSize < size && idxGlobal >= 0) {
            String msg = "In insert Syntax, you can't set value for Global check column!";
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }

        sb.append("(");
        int iValue = 0;
        //put the value number into string buffer
        for (int i = 0; i < colSize; i++) {
            if (i == idxGlobal) {
                sb.append(String.valueOf(new Date().getTime()));
            } else if (i == autoIncrement) {
                if (checkSize > size) {
                    long id = DbleServer.getInstance().getSequenceHandler().nextId(tableKey);
                    sb.append(id);
                } else {
                    String value = SQLUtils.toMySqlString(valuse.get(iValue++));
                    sb.append(value);
                }
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


    private RouteResultset parserChildTable(SchemaUtil.SchemaInfo schemaInfo, RouteResultset rrs, MySqlReplaceStatement replace, ServerConnection sc) throws SQLNonTransientException {
        SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        TableConfig tc = schema.getTables().get(tableName);
        //check if the childtable replace with the multi
        if (isMultiReplace(replace)) {
            String msg = "ChildTable multi insert not provided";
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        //find the value of child table join key
        String joinKey = tc.getJoinKey();
        int joinKeyIndex = getJoinKeyIndex(schemaInfo, replace, joinKey);
        String joinKeyVal = replace.getValuesList().get(0).getValues().get(joinKeyIndex).toString();
        String realVal = StringUtil.removeApostrophe(joinKeyVal);
        String sql = RouterUtil.removeSchema(replace.toString(), schemaInfo.getSchema());
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

    private boolean isMultiReplace(MySqlReplaceStatement insertStmt) {
        return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1);
    }

    /**
     * find the index of the joinkey
     *
     * @param schemaInfo
     * @param replaceStmt
     * @param joinKey
     * @return -1 not found
     * @throws SQLNonTransientException
     */
    private int getJoinKeyIndex(SchemaUtil.SchemaInfo schemaInfo, MySqlReplaceStatement replaceStmt, String joinKey) throws SQLNonTransientException {
        return getShardingColIndex(schemaInfo, replaceStmt, joinKey);
    }


    /**
     * find the index of the key in column list
     *
     * @param insertStmt
     * @param partitionColumn
     * @return
     * @throws SQLNonTransientException
     */
    private int getShardingColIndex(SchemaUtil.SchemaInfo schemaInfo, MySqlReplaceStatement insertStmt, String partitionColumn) throws SQLNonTransientException {
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


    /**
     * insert into .... select .... OR insert into table() values (),(),....
     *
     * @param schemaInfo
     * @param rrs
     * @param partitionColumn
     * @param replace
     * @throws SQLNonTransientException
     */
    private void parserBatchInsert(SchemaUtil.SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                   MySqlReplaceStatement replace) throws SQLNonTransientException {
        // insert into table() values (),(),....
        SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        // column number
        int columnNum = getTableColumns(schemaInfo, replace);
        int shardingColIndex = getShardingColIndex(schemaInfo, replace, partitionColumn);
        List<SQLInsertStatement.ValuesClause> valueClauseList = replace.getValuesList();
        Map<Integer, List<SQLInsertStatement.ValuesClause>> nodeValuesMap = new HashMap<>();
        TableConfig tableConfig = schema.getTables().get(tableName);
        AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
        for (SQLInsertStatement.ValuesClause valueClause : valueClauseList) {
            if (valueClause.getValues().size() != columnNum) {
                String msg = "bad insert sql columnSize != valueSize:" + columnNum + " != " + valueClause.getValues().size() + "values:" + valueClause;
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            SQLExpr expr = valueClause.getValues().get(shardingColIndex);
            String shardingValue = ReplaceInsertUtil.shardingValueToSting(expr);
            Integer nodeIndex = algorithm.calculate(shardingValue);
            // no part find for this record
            if (nodeIndex == null) {
                String msg = "can't find any valid datanode :" + tableName + " -> " + partitionColumn + " -> " + shardingValue;
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            if (nodeValuesMap.get(nodeIndex) == null) {
                nodeValuesMap.put(nodeIndex, new ArrayList<SQLInsertStatement.ValuesClause>());
            }
            nodeValuesMap.get(nodeIndex).add(valueClause);
        }

        RouteResultsetNode[] nodes = new RouteResultsetNode[nodeValuesMap.size()];
        int count = 0;
        for (Map.Entry<Integer, List<SQLInsertStatement.ValuesClause>> node : nodeValuesMap.entrySet()) {
            Integer nodeIndex = node.getKey();
            List<SQLInsertStatement.ValuesClause> valuesList = node.getValue();
            ReplaceTemp temp = new ReplaceTemp(replace);
            temp.setValuesList(valuesList);
            nodes[count] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex), rrs.getSqlType(),
                    RouterUtil.removeSchema(temp.toString(), schemaInfo.getSchema()));
            count++;
        }
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }


    private int getTableColumns(SchemaUtil.SchemaInfo schemaInfo, MySqlReplaceStatement replaceStatement)
            throws SQLNonTransientException {
        if (replaceStatement.getColumns() == null || replaceStatement.getColumns().size() == 0) {
            StructureMeta.TableMeta tbMeta = DbleServer.getInstance().getTmManager().getSyncTableMeta(schemaInfo.getSchema(), schemaInfo.getTable());
            if (tbMeta == null) {
                String msg = "Meta data of table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist";
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            return tbMeta.getColumnsCount();
        } else {
            return replaceStatement.getColumns().size();
        }
    }


    /**
     * insert single record
     *
     * @param schemaInfo
     * @param rrs
     * @param partitionColumn
     * @param replaceStatement
     * @throws SQLNonTransientException
     */
    private void parserSingleInsert(SchemaUtil.SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                    MySqlReplaceStatement replaceStatement) throws SQLNonTransientException {
        int shardingColIndex = getShardingColIndex(schemaInfo, replaceStatement, partitionColumn);
        SQLExpr valueExpr = replaceStatement.getValuesList().get(0).getValues().get(shardingColIndex);
        String shardingValue = ReplaceInsertUtil.shardingValueToSting(valueExpr);
        TableConfig tableConfig = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
        AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
        Integer nodeIndex = algorithm.calculate(shardingValue);
        if (nodeIndex == null) {
            String msg = "can't find any valid datanode :" + schemaInfo.getTable() + " -> " + partitionColumn + " -> " + shardingValue;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex),
                rrs.getSqlType(), RouterUtil.removeSchema(replaceStatement.toString(), schemaInfo.getSchema()));

        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }
}
