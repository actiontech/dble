/*
 * Copyright (C) 2016-2019 ActionTech.
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
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.plan.common.ptr.StringPtr;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.handler.ExplainHandler;
import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * Created by szf on 2017/8/18.
 */
public class DruidReplaceParser extends DruidInsertReplaceParser {

    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ServerConnection sc, boolean isExplain)
            throws SQLException {
        //data & object prepare
        SQLReplaceStatement replace = (SQLReplaceStatement) stmt;
        String schemaName = schema == null ? null : schema.getName();
        SQLExprTableSource tableSource = replace.getTableSource();
        SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, tableSource);

        //privilege check
        if (!ServerPrivileges.checkPrivilege(sc, schemaInfo.getSchema(), schemaInfo.getTable(), ServerPrivileges.CheckType.INSERT)) {
            String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
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
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }

        //check the config of target table
        TableConfig tc = schema.getTables().get(tableName);
        checkTableExists(tc, schema.getName(), tableName, ServerPrivileges.CheckType.INSERT);

        //if the target table is global table than
        if (tc.isGlobalTable()) {
            String sql = rrs.getStatement();
            if (tc.isAutoIncrement() || GlobalTableUtil.useGlobalTableCheck()) {
                sql = convertReplaceSQL(schemaInfo, replace, sql, tc, GlobalTableUtil.useGlobalTableCheck(), sc);
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
            replace = (SQLReplaceStatement) stmt;
        }

        // childTable can be route in this part
        if (tc.getParentTC() != null) {
            parserChildTable(schemaInfo, rrs, replace, sc, isExplain);
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
    private boolean parserNoSharding(ServerConnection sc, String contextSchema, SchemaInfo schemaInfo, RouteResultset rrs, SQLReplaceStatement replace) throws SQLException {
        String noShardingNode = RouterUtil.isNoSharding(schemaInfo.getSchemaConfig(), schemaInfo.getTable());
        if (noShardingNode != null) {
            StringPtr noShardingNodePr = new StringPtr(noShardingNode);
            Set<String> schemas = new HashSet<>();
            if (replace.getQuery() != null) {
                //replace into ...select  if the both table is nosharding table
                SQLSelect select = replace.getQuery().getSubQuery();
                SQLSelectStatement selectStmt = new SQLSelectStatement(select);
                if (!SchemaUtil.isNoSharding(sc, select.getQuery(), replace, selectStmt, contextSchema, schemas, noShardingNodePr)) {
                    return false;
                }
            }
            routeToNoSharding(schemaInfo.getSchemaConfig(), rrs, schemas, noShardingNodePr);
            return true;
        }
        return false;
    }


    private String convertReplaceSQL(SchemaInfo schemaInfo, SQLReplaceStatement replace, String originSql, TableConfig tc, boolean isGlobalCheck, ServerConnection sc) throws SQLNonTransientException {
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
                autoIncrement = getIncrementKeyIndex(schemaInfo, tc.getTrueIncrementColumn());
            }
            colSize = orgTbMeta.getColumnsList().size();
            idxGlobal = getIdxGlobalByMeta(isGlobalCheck, orgTbMeta, sb, colSize);
        } else { // replace sql with  column names
            boolean hasIncrementInSql = concatColumns(replace, tc, isGlobalCheck, isAutoIncrement, sb, columns);
            colSize = columns.size();
            if (isAutoIncrement && !hasIncrementInSql) {
                getIncrementKeyIndex(schemaInfo, tc.getTrueIncrementColumn());
                autoIncrement = columns.size();
                sb.append(",").append(tc.getTrueIncrementColumn());
                colSize++;
            }
            if (isGlobalCheck) {
                idxGlobal = (isAutoIncrement && !hasIncrementInSql) ? columns.size() + 1 : columns.size();
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
            List<SQLExpr> values = replace.getValuesList().get(0).getValues();
            appendValues(tableKey, values, sb, autoIncrement, idxGlobal, colSize);
        }

        return RouterUtil.removeSchema(sb.toString(), schemaInfo.getSchema());
    }

    private boolean concatColumns(SQLReplaceStatement replace, TableConfig tc, boolean isGlobalCheck, boolean isAutoIncrement, StringBuilder sb, List<SQLExpr> columns) throws SQLNonTransientException {
        sb.append("(");
        boolean hasIncrementInSql = false;
        for (int i = 0; i < columns.size(); i++) {
            if (isAutoIncrement && columns.get(i).toString().equalsIgnoreCase(tc.getTrueIncrementColumn())) {
                hasIncrementInSql = true;
            }
            if (i < columns.size() - 1)
                sb.append(columns.get(i).toString()).append(",");
            else
                sb.append(columns.get(i).toString());
            String column = StringUtil.removeBackQuote(replace.getColumns().get(i).toString());
            if (isGlobalCheck && column.equalsIgnoreCase(GlobalTableUtil.GLOBAL_TABLE_CHECK_COLUMN)) {
                String msg = "In insert Syntax, you can't set value for Global check column!";
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
        }
        return hasIncrementInSql;
    }


    /**
     * because of the replace can use a
     *
     * @param tableKey
     * @param values
     * @param sb
     * @param autoIncrement
     * @param idxGlobal
     * @param colSize
     * @return
     * @throws SQLNonTransientException
     */
    private static StringBuilder appendValues(String tableKey, List<SQLExpr> values, StringBuilder sb,
                                              int autoIncrement, int idxGlobal, int colSize) throws SQLNonTransientException {
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
                sb.append(String.valueOf(new Date().getTime()));
            } else if (i == autoIncrement) {
                if (checkSize > size) {
                    long id = DbleServer.getInstance().getSequenceHandler().nextId(tableKey);
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


    private void parserChildTable(SchemaInfo schemaInfo, final RouteResultset rrs, SQLReplaceStatement replace, final ServerConnection sc, boolean isExplain) throws SQLNonTransientException {
        final SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        final TableConfig tc = schema.getTables().get(tableName);
        //check if the childtable replace with the multi
        if (isMultiReplace(replace)) {
            String msg = "ChildTable multi insert not provided";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        //find the value of child table join key
        String joinKey = tc.getJoinKey();
        int joinKeyIndex = getJoinKeyIndex(schemaInfo, replace, joinKey);
        final String joinKeyVal = replace.getValuesList().get(0).getValues().get(joinKeyIndex).toString();
        String realVal = StringUtil.removeApostrophe(joinKeyVal);
        final String sql = RouterUtil.removeSchema(statementToString(replace), schemaInfo.getSchema());
        rrs.setStatement(sql);
        // try to route by ER parent partition key
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
                    try {
                        String dn = fetchHandler.execute(schema.getName(), tc.getRootParent().getDataNodes());
                        if (dn == null) {
                            sc.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "can't find (root) parent sharding node for sql:" + sql);
                            return;
                        }
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("found partition node for child table to insert " + dn + " sql :" + sql);
                        }
                        RouterUtil.routeToSingleNode(rrs, dn);
                        if (isExplain) {
                            ExplainHandler.writeOutHeadAndEof(sc, rrs);
                        } else {
                            sc.getSession2().execute(rrs);
                        }
                    } catch (ConnectionException e) {
                        sc.setTxInterrupt(e.toString());
                        sc.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, e.toString());
                    }
                }
            });
        }
    }

    private boolean isMultiReplace(SQLReplaceStatement insertStmt) {
        return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1);
    }

    /**
     * find joinKey index
     *
     * @param schemaInfo  SchemaInfo
     * @param replaceStmt MySqlInsertStatement
     * @param joinKey     joinKey
     * @return -1 means no join key,otherwise means the index
     * @throws SQLNonTransientException if not find
     */
    private int getJoinKeyIndex(SchemaInfo schemaInfo, SQLReplaceStatement replaceStmt, String joinKey) throws SQLNonTransientException {
        return tryGetShardingColIndex(schemaInfo, replaceStmt, joinKey);
    }


    /**
     * find the index of the key in column list
     *
     * @param schemaInfo      SchemaInfo
     * @param replaceStmt     SQLReplaceStatement
     * @param partitionColumn partitionColumn
     * @return the index of the partition column
     * @throws SQLNonTransientException if not find
     */
    private int tryGetShardingColIndex(SchemaInfo schemaInfo, SQLReplaceStatement replaceStmt, String partitionColumn) throws SQLNonTransientException {
        int shardingColIndex = getShardingColIndex(schemaInfo, replaceStmt.getColumns(), partitionColumn);
        if (shardingColIndex != -1) return shardingColIndex;
        throw new SQLNonTransientException("bad insert sql, sharding column/joinKey:" + partitionColumn + " not provided," + replaceStmt);
    }

    /**
     * insert into .... select .... OR insert into table() values (),(),....
     *
     * @param schemaInfo      SchemaInfo
     * @param rrs             RouteResultset
     * @param partitionColumn partitionColumn
     * @param replace         SQLReplaceStatement
     * @throws SQLNonTransientException if the column size of values is not correct
     */
    private void parserBatchInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                   SQLReplaceStatement replace) throws SQLNonTransientException {
        // insert into table() values (),(),....
        SchemaConfig schema = schemaInfo.getSchemaConfig();
        String tableName = schemaInfo.getTable();
        // column number
        int columnNum = getTableColumns(schemaInfo, replace.getColumns());
        int shardingColIndex = tryGetShardingColIndex(schemaInfo, replace, partitionColumn);
        List<SQLInsertStatement.ValuesClause> valueClauseList = replace.getValuesList();
        Map<Integer, List<SQLInsertStatement.ValuesClause>> nodeValuesMap = new HashMap<>();
        TableConfig tableConfig = schema.getTables().get(tableName);
        AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
        for (SQLInsertStatement.ValuesClause valueClause : valueClauseList) {
            if (valueClause.getValues().size() != columnNum) {
                String msg = "bad insert sql columnSize != valueSize:" + columnNum + " != " + valueClause.getValues().size() + "values:" + valueClause;
                LOGGER.info(msg);
                throw new SQLNonTransientException(msg);
            }
            SQLExpr expr = valueClause.getValues().get(shardingColIndex);
            String shardingValue = shardingValueToSting(expr);
            Integer nodeIndex = algorithm.calculate(shardingValue);
            // no part find for this record
            if (nodeIndex == null) {
                String msg = "can't find any valid datanode :" + tableName + " -> " + partitionColumn + " -> " + shardingValue;
                LOGGER.info(msg);
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
            replace.getValuesList().clear();
            replace.getValuesList().addAll(valuesList);
            nodes[count] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex), rrs.getSqlType(),
                    RouterUtil.removeSchema(statementToString(replace), schemaInfo.getSchema()));
            count++;
        }
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }


    /**
     * insert single record
     *
     * @param schemaInfo       SchemaInfo
     * @param rrs              RouteResultset
     * @param partitionColumn  partitionColumn
     * @param replaceStatement SQLReplaceStatement
     * @throws SQLNonTransientException if not find a valid data node
     */
    private void parserSingleInsert(SchemaInfo schemaInfo, RouteResultset rrs, String partitionColumn,
                                    SQLReplaceStatement replaceStatement) throws SQLNonTransientException {
        int shardingColIndex = tryGetShardingColIndex(schemaInfo, replaceStatement, partitionColumn);
        SQLExpr valueExpr = replaceStatement.getValuesList().get(0).getValues().get(shardingColIndex);
        String shardingValue = shardingValueToSting(valueExpr);
        TableConfig tableConfig = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
        AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
        Integer nodeIndex = algorithm.calculate(shardingValue);
        if (nodeIndex == null) {
            String msg = "can't find any valid data node :" + schemaInfo.getTable() + " -> " + partitionColumn + " -> " + shardingValue;
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }
        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex),
                rrs.getSqlType(), RouterUtil.removeSchema(statementToString(replaceStatement), schemaInfo.getSchema()));

        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
    }
}
