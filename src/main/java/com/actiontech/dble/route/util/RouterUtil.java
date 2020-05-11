/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.DruidParser;
import com.actiontech.dble.route.parser.druid.DruidShardingParseInfo;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.sqlengine.mpp.ColumnRoute;
import com.actiontech.dble.sqlengine.mpp.RangeValue;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.wall.spi.WallVisitorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery.AllAnySubQueryHandler.*;
import static com.actiontech.dble.plan.optimizer.JoinStrategyProcessor.NEED_REPLACE;

/**
 * ServerRouterUtil
 *
 * @author wang.dw
 */
public final class RouterUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouterUtil.class);
    private static ThreadLocalRandom rand = ThreadLocalRandom.current();

    private RouterUtil() {
    }

    public static String removeSchema(String stmt, String schema) {
        return removeSchema(stmt, schema, DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames());
    }

    /**
     * removeSchema from sql
     *
     * @param stmt        sql
     * @param schema      has change to lowercase if need
     * @param isLowerCase lowercase
     * @return new sql
     */
    public static String removeSchema(String stmt, String schema, boolean isLowerCase) {
        final String forCmpStmt = isLowerCase ? stmt.toLowerCase() : stmt;
        final String maySchema1 = schema + ".";
        final String maySchema2 = "`" + schema + "`.";
        int index1 = forCmpStmt.indexOf(maySchema1);
        int index2 = forCmpStmt.indexOf(maySchema2);
        if (index1 < 0 && index2 < 0) {
            return stmt;
        }
        int startPos = 0;
        boolean flag;
        int firstE = forCmpStmt.indexOf("'");
        int endE = forCmpStmt.lastIndexOf("'");
        StringBuilder result = new StringBuilder();
        while (index1 >= 0 || index2 >= 0) {
            //match `schema` or `schema`
            if (index1 < 0 && index2 >= 0) {
                flag = true;
            } else if (index1 >= 0 && index2 < 0) {
                flag = false;
            } else {
                flag = index2 < index1;
            }
            if (flag) {
                result.append(stmt, startPos, index2);
                startPos = index2 + maySchema2.length();
                if (index2 > firstE && index2 < endE && countChar(stmt, index2) % 2 != 0) {
                    result.append(stmt, index2, startPos);
                }
                index2 = forCmpStmt.indexOf(maySchema2, startPos);
            } else {
                result.append(stmt, startPos, index1);
                startPos = index1 + maySchema1.length();
                if (index1 > firstE && index1 < endE && countChar(stmt, index1) % 2 != 0) {
                    result.append(stmt, index1, startPos);
                }
                index1 = forCmpStmt.indexOf(maySchema1, startPos);
            }
        }
        result.append(stmt.substring(startPos));
        return result.toString();
    }

    private static int countChar(String sql, int end) {
        int count = 0;
        boolean skipChar = false;
        for (int i = 0; i < end; i++) {
            if (sql.charAt(i) == '\'' && !skipChar) {
                count++;
                skipChar = false;
            } else skipChar = sql.charAt(i) == '\\';
        }
        return count;
    }


    public static RouteResultset routeFromParserComplex(
            SchemaConfig schema, DruidParser druidParser, Map<Pair<String, String>, SchemaConfig> schemaMap,
            RouteResultset rrs, SQLStatement statement,
            ServerSchemaStatVisitor visitor, ServerConnection sc) throws SQLException {
        druidParser.parser(schema, rrs, statement, visitor, sc);
        if (rrs.isFinishedExecute()) {
            return null;
        }
        if (rrs.isFinishedRoute()) {
            return rrs;
        }

        /* multi-tables for broadcast */
        if (druidParser.getCtx().getRouteCalculateUnits().size() == 0) {
            RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
            druidParser.getCtx().addRouteCalculateUnit(routeCalculateUnit);
        }

        SortedSet<RouteResultsetNode> nodeSet = new TreeSet<>();
        for (RouteCalculateUnit unit : druidParser.getCtx().getRouteCalculateUnits()) {
            if (unit.isAlwaysFalse()) {
                rrs.setAlwaysFalse(true);
            }
            RouteResultset rrsTmp = RouterUtil.tryRouteForTablesComplex(schemaMap, druidParser.getCtx(), unit, rrs);
            if (rrsTmp != null && (rrsTmp.getNodes() != null || rrsTmp.getNodes().length != 0)) {
                Collections.addAll(nodeSet, rrsTmp.getNodes());
                if (rrsTmp.isGlobalTable()) {
                    break;
                }
            }
        }

        RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
        int i = 0;
        for (RouteResultsetNode aNodeSet : nodeSet) {
            nodes[i] = aNodeSet;
            i++;
        }
        rrs.setNodes(nodes);

        return rrs;
    }


    public static RouteResultset routeFromParser(DruidParser druidParser, SchemaConfig schema, RouteResultset rrs, SQLStatement statement,
                                                 ServerSchemaStatVisitor visitor,
                                                 ServerConnection sc, boolean isExplain) throws SQLException {
        schema = druidParser.parser(schema, rrs, statement, visitor, sc, isExplain);
        if (rrs.isFinishedExecute()) {
            return null;
        }
        if (rrs.isFinishedRoute()) {
            return rrs;
        }

        /*
         * no name table or others
         */
        DruidShardingParseInfo ctx = druidParser.getCtx();
        if ((ctx.getTables() == null || ctx.getTables().size() == 0) &&
                (ctx.getTableAliasMap() == null || ctx.getTableAliasMap().isEmpty())) {
            if (schema == null) {
                schema = DbleServer.getInstance().getConfig().getSchemas().get(SchemaUtil.getRandomDb());
            }
            return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode());
        }

        /* multi-tables*/
        if (druidParser.getCtx().getRouteCalculateUnits().size() == 0) {
            RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
            druidParser.getCtx().addRouteCalculateUnit(routeCalculateUnit);
        }

        SortedSet<RouteResultsetNode> nodeSet = new TreeSet<>();
        for (RouteCalculateUnit unit : druidParser.getCtx().getRouteCalculateUnits()) {
            if (unit.isAlwaysFalse()) {
                rrs.setAlwaysFalse(true);
            }
            RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, druidParser.getCtx(), unit, rrs, isSelect(statement));
            if (rrsTmp != null && (rrsTmp.getNodes() != null || rrsTmp.getNodes().length != 0)) {
                Collections.addAll(nodeSet, rrsTmp.getNodes());
                if (rrsTmp.isGlobalTable()) {
                    break;
                }
            }
        }


        RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
        int i = 0;
        for (RouteResultsetNode aNodeSet : nodeSet) {
            nodes[i] = aNodeSet;
            i++;
        }
        rrs.setNodes(nodes);


        return rrs;
    }

    /**
     * isSelect
     */
    private static boolean isSelect(SQLStatement statement) {
        return statement instanceof SQLSelectStatement;
    }

    public static void routeToSingleDDLNode(SchemaInfo schemaInfo, RouteResultset rrs, String dataNode) throws SQLException {
        rrs.setSchema(schemaInfo.getSchema());
        rrs.setTable(schemaInfo.getTable());
        RouterUtil.routeToSingleNode(rrs, dataNode);
    }

    public static void routeNoNameTableToSingleNode(RouteResultset rrs, SchemaConfig schema) throws SQLNonTransientException {
        if (schema == null) {
            String db = SchemaUtil.getRandomDb();
            if (db == null) {
                String msg = "No schema is configured, make sure your config is right, sql:" + rrs.getStatement();
                throw new SQLNonTransientException(msg);
            }
            schema = DbleServer.getInstance().getConfig().getSchemas().get(db);
        }
        rrs = RouterUtil.routeToSingleNode(rrs, schema.getMetaDataNode());
        rrs.setFinishedRoute(true);
    }

    /**
     * the first node as the result
     *
     * @param rrs      RouteResultset
     * @param dataNode NAME
     * @return RouteResultset
     */
    public static RouteResultset routeToSingleNode(RouteResultset rrs, String dataNode) {
        if (dataNode == null) {
            return rrs;
        }
        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), rrs.getStatement());
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
        if (rrs.getCanRunInReadDB() != null) {
            nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
        }
        if (rrs.getRunOnSlave() != null) {
            nodes[0].setRunOnSlave(rrs.getRunOnSlave());
        }

        return rrs;
    }


    public static void routeToDDLNode(SchemaInfo schemaInfo, RouteResultset rrs) throws SQLException {
        String stmt = getFixedSql(removeSchema(rrs.getStatement(), schemaInfo.getSchema()));
        List<String> dataNodes;
        Map<String, TableConfig> tables = schemaInfo.getSchemaConfig().getTables();
        TableConfig tc = tables.get(schemaInfo.getTable());
        if (tc != null) {
            dataNodes = tc.getDataNodes();
        } else {
            String msg = "Table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }
        Iterator<String> iterator1 = dataNodes.iterator();
        int nodeSize = dataNodes.size();
        RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSize];

        for (int i = 0; i < nodeSize; i++) {
            String name = iterator1.next();
            nodes[i] = new RouteResultsetNode(name, ServerParse.DDL, stmt);
        }
        rrs.setNodes(nodes);
        rrs.setSchema(schemaInfo.getSchema());
        rrs.setTable(schemaInfo.getTable());
        rrs.setFinishedRoute(true);
    }


    /**
     * getFixedSql
     *
     * @param stmt sql
     * @return FixedSql
     * @author AStoneGod
     */
    public static String getFixedSql(String stmt) {
        return stmt.replaceAll("[\\t\\n\\r]", " ").trim();
    }


    private static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs, Collection<String> dataNodes) {
        RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
        int i = 0;
        RouteResultsetNode node;
        for (String dataNode : dataNodes) {
            node = new RouteResultsetNode(dataNode, rrs.getSqlType(), rrs.getStatement());
            if (rrs.getCanRunInReadDB() != null) {
                node.setCanRunInReadDB(rrs.getCanRunInReadDB());
            }
            if (rrs.getRunOnSlave() != null) {
                nodes[0].setRunOnSlave(rrs.getRunOnSlave());
            }
            nodes[i++] = node;
        }
        rrs.setSqlRouteCacheAble(cache);
        rrs.setNodes(nodes);
        return rrs;
    }

    public static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs, Collection<String> dataNodes, boolean isGlobalTable) {
        rrs = routeToMultiNode(cache, rrs, dataNodes);
        rrs.setGlobalTable(isGlobalTable);
        return rrs;
    }

    public static void routeToRandomNode(RouteResultset rrs,
                                         SchemaConfig schema, String tableName) throws SQLException {
        String dataNode = getRandomDataNode(schema, tableName);
        routeToSingleNode(rrs, dataNode);
    }

    public static String getRandomDataNode(ArrayList<String> dataNodes) {
        int index = rand.nextInt(dataNodes.size());
        ArrayList<String> x = new ArrayList<>(dataNodes);
        Map<String, PhysicalDataNode> dataNodeMap = DbleServer.getInstance().getConfig().getDataNodes();
        while (x.size() > 1) {
            if (dataNodeMap.get(x.get(index)).getDataHost().getWriteSource().isAlive()) {
                return x.get(index);
            }
            x.remove(index);
            index = rand.nextInt(x.size());
        }

        return x.get(0);
    }

    private static String getRandomDataNode(SchemaConfig schema, String table) throws SQLException {
        Map<String, TableConfig> tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(table)) != null) {
            return tc.getRandomDataNode();
        } else {
            String msg = "Table '" + schema.getName() + "." + table + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }
    }

    private static Set<String> ruleByJoinValueCalculate(RouteResultset rrs, TableConfig tc,
                                                        ColumnRoute colRoutePairSet) throws SQLNonTransientException {
        Set<String> retNodeSet = new LinkedHashSet<>();
        if (tc.getDirectRouteTC() != null) {
            Set<String> nodeSet = ruleCalculate(rrs, tc.getDirectRouteTC(), colRoutePairSet, false);
            if (nodeSet.isEmpty()) {
                throw new SQLNonTransientException("parent key can't find  valid data node ,expect 1 but found: " + nodeSet.size());
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found partition node (using parent partition rule directly) for child table to insert  " + nodeSet + " sql :" + rrs.getStatement());
            }
            retNodeSet.addAll(nodeSet);
            return retNodeSet;
        } else {
            retNodeSet.addAll(tc.getParentTC().getDataNodes());
        }
        return retNodeSet;
    }

    public static Set<String> ruleCalculate(RouteResultset rrs, TableConfig tc, ColumnRoute columnRoute, boolean ignoreNull) {
        Set<String> routeNodeSet = new LinkedHashSet<>();
        if (columnRoute.getColValue() != null) {
            String value = columnRoute.getColValue();
            //for explain
            if (NEED_REPLACE.equals(value) || ALL_SUB_QUERY_RESULTS.equals(value) ||
                    MIN_SUB_QUERY_RESULTS.equals(value) || MAX_SUB_QUERY_RESULTS.equals(value)) {
                return routeNodeSet;
            }
            if (!ignoreNull || !columnRoute.getColValue().equalsIgnoreCase("null")) {
                String dataNode = ruleCalculateSingleValue(tc, columnRoute.getColValue());
                routeNodeSet.add(dataNode);
            }
        } else if (columnRoute.getInValues() != null) {
            for (String value : columnRoute.getInValues()) {
                String dataNode = ruleCalculateSingleValue(tc, value);
                routeNodeSet.add(dataNode);
            }
        }
        if (columnRoute.getRangeValues() != null) {
            Set<String> rangeNodeSet = new LinkedHashSet<>();
            boolean isFirst = true;
            for (RangeValue rangeValue : columnRoute.getRangeValues()) { // get Intersection from all between and
                Integer[] nodeRange = tc.getRule().getRuleAlgorithm().calculateRange(String.valueOf(rangeValue.getBeginValue()), String.valueOf(rangeValue.getEndValue()));
                if (nodeRange != null) {
                    if (isFirst) {
                        if (nodeRange.length == 0) {
                            rangeNodeSet.addAll(tc.getDataNodes());
                        } else {
                            String dataNode;
                            for (Integer nodeId : nodeRange) {
                                dataNode = tc.getDataNodes().get(nodeId);
                                rangeNodeSet.add(dataNode);
                            }
                        }
                        isFirst = false;
                    } else {
                        if (nodeRange.length == 0) {
                            rangeNodeSet.retainAll(tc.getDataNodes());
                        } else {
                            String dataNode;
                            Set<String> tmpNodeSet = new LinkedHashSet<>();
                            for (Integer nodeId : nodeRange) {
                                dataNode = tc.getDataNodes().get(nodeId);
                                tmpNodeSet.add(dataNode);
                            }
                            rangeNodeSet.retainAll(tmpNodeSet);
                        }
                        if (rangeNodeSet.size() == 0) { //all between and is always false
                            break;
                        }
                    }
                }
            }
            if (routeNodeSet.size() != 0) {
                routeNodeSet.retainAll(rangeNodeSet);
            } else {
                routeNodeSet.addAll(rangeNodeSet);
            }
            if (routeNodeSet.size() == 0) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("all ColumnRoute " + columnRoute + " merge to always false");
                }
                rrs.setAlwaysFalse(true);
                rangeNodeSet.addAll(tc.getDataNodes());
            }
        }
        return routeNodeSet;
    }

    private static String ruleCalculateSingleValue(TableConfig tc, String value) {
        Integer nodeIndex = tc.getRule().getRuleAlgorithm().calculate(value);
        if (nodeIndex == null) {
            String msg = "can't find any valid data node in table[" + tc.getName() +
                    "] -> column[" + tc.getPartitionColumn() + "] -> value[" + value + "]";
            LOGGER.info(msg);
            throw new IllegalArgumentException(msg);
        }
        if (nodeIndex < 0 || nodeIndex >= tc.getDataNodes().size()) {
            String msg = "Can't find a valid data node for specified node index in table[" + tc.getName() +
                    "] -> column[" + tc.getPartitionColumn() + "] -> value[" + value + "]" + ",Index : " + nodeIndex;
            LOGGER.info(msg);
            throw new IllegalArgumentException(msg);
        }
        return tc.getDataNodes().get(nodeIndex);
    }

    public static String tryRouteTablesToOneNodeForComplex(RouteResultset rrs, DruidShardingParseInfo ctx, Set<String> schemaList, int tableSize) throws SQLException {
        if (ctx.getTables().size() != tableSize) {
            return null;
        }
        Set<String> tmpResultNodes = new HashSet<>();

        Set<Pair<String, String>> tablesSet = new HashSet<>(ctx.getTables());
        Set<Pair<String, TableConfig>> globalTables = new HashSet<>();
        for (Pair<String, String> table : ctx.getTables()) {
            String schemaName = table.getKey();
            String tableName = table.getValue();
            SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
            schemaList.add(schemaName);
            TableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig == null) {
                if (tryRouteNoShardingTablesToOneNode(tmpResultNodes, tablesSet, table, schemaName, tableName, schema))
                    return null;
            } else if (tableConfig.isGlobalTable()) {
                globalTables.add(new Pair<>(schemaName, tableConfig));
            } else if (schema.getTables().get(tableName).getDataNodes().size() == 1) {
                tmpResultNodes.add(schema.getTables().get(tableName).getDataNodes().get(0));
                tablesSet.remove(table);
                if (tmpResultNodes.size() != 1) {
                    return null;
                }
            }
        }
        if (globalTables.size() == tableSize) {
            return tryRouteGlobalTablesToOneNode(tmpResultNodes, globalTables);
        }
        if (tablesSet.size() != 0) {
            Set<String> resultNodes = new HashSet<>();
            for (RouteCalculateUnit routeUnit : ctx.getRouteCalculateUnits()) {
                if (routeUnit.isAlwaysFalse()) {
                    rrs.setAlwaysFalse(true);
                }
                Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = routeUnit.getTablesAndConditions();
                if (tablesAndConditions != null) {
                    for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : tablesAndConditions.entrySet()) {
                        Pair<String, String> table = entry.getKey();
                        String schemaName = table.getKey();
                        String tableName = table.getValue();
                        SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
                        TableConfig tableConfig = schema.getTables().get(tableName);
                        if (!tryCalcNodeForShardingColumn(rrs, tmpResultNodes, tablesSet, entry, table, tableConfig)) {
                            return null;
                        }
                    }
                }
                for (Pair<String, TableConfig> table : globalTables) {
                    TableConfig tb = table.getValue();
                    tmpResultNodes.retainAll(tb.getDataNodes());
                    tablesSet.remove(new Pair<>(table.getKey(), tb.getName()));
                }
                if (tmpResultNodes.size() != 1 || tablesSet.size() != 0) {
                    return null;
                }
                resultNodes.add(tmpResultNodes.iterator().next());
                if (resultNodes.size() != 1) {
                    return null;
                }
            }
            if (resultNodes.size() != 1) {
                return null;
            }
            return resultNodes.iterator().next();
        } else {
            if (tmpResultNodes.size() != 1) {
                return null;
            }
            return tmpResultNodes.iterator().next();
        }

    }

    private static String tryRouteGlobalTablesToOneNode(Set<String> tmpResultNodes, Set<Pair<String, TableConfig>> globalTables) {
        boolean isFirstTable = true;
        for (Pair<String, TableConfig> table : globalTables) {
            TableConfig tb = table.getValue();
            if (isFirstTable) {
                tmpResultNodes.addAll(tb.getDataNodes());
                isFirstTable = false;
            } else {
                tmpResultNodes.retainAll(tb.getDataNodes());
            }
        }
        if (tmpResultNodes.size() != 0) {
            return getRandomDataNode(new ArrayList<>(tmpResultNodes));
        } else {
            return null;
        }
    }

    private static boolean tryRouteNoShardingTablesToOneNode(Set<String> tmpResultNodes, Set<Pair<String, String>> tablesSet, Pair<String, String> table, String schemaName, String tableName, SchemaConfig schema) throws SQLNonTransientException {
        //may view
        if (ProxyMeta.getInstance().getTmManager().getSyncView(schemaName, tableName) != null) {
            return true;
        }
        if (schema.getDataNode() == null) {
            String msg = " Table '" + schemaName + "." + tableName + "' doesn't exist";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        } else {
            tmpResultNodes.add(schema.getDataNode());
            tablesSet.remove(table);
            if (tmpResultNodes.size() != 1) {
                return true;
            }
        }
        return false;
    }

    private static boolean tryCalcNodeForShardingColumn(
            RouteResultset rrs, Set<String> resultNodes, Set<Pair<String, String>> tablesSet,
            Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry, Pair<String, String> table,
            TableConfig tableConfig) throws SQLNonTransientException {
        if (tableConfig == null) {
            return false; //  alias table, may subquery
        }
        if (tableConfig.getPartitionColumn() == null) {
            return true;
        }

        String joinKey = tableConfig.getJoinKey();
        String partitionCol = tableConfig.getPartitionColumn();

        // where filter contains partition column

        Map<String, ColumnRoute> columnsMap = entry.getValue();
        if (partitionCol != null && columnsMap.get(partitionCol) != null) {
            ColumnRoute partitionValue = columnsMap.get(partitionCol);
            try {
                Set<String> dataNodeSet = ruleCalculate(rrs, tableConfig, partitionValue, false);
                resultNodes.addAll(dataNodeSet);
            } catch (Exception e) { //complex filter
                return true;
            }
            tablesSet.remove(table);
            if (resultNodes.size() != 1) {
                return false;
            }
        } else if (joinKey != null && columnsMap.get(joinKey) != null) {
            ColumnRoute joinKeyValue = columnsMap.get(joinKey);
            Set<String> dataNodeSet = ruleByJoinValueCalculate(rrs, tableConfig, joinKeyValue);
            if (dataNodeSet.size() > 1) {
                return false;
            }
            resultNodes.addAll(dataNodeSet);
            tablesSet.remove(table);
            return resultNodes.size() == 1;
        } else {
            return false;
        }
        return true;
    }

    /**
     * tryRouteFor multiTables
     */
    private static RouteResultset tryRouteForTables(
            SchemaConfig schema, DruidShardingParseInfo ctx, RouteCalculateUnit routeUnit, RouteResultset rrs,
            boolean isSelect) throws SQLException {
        List<Pair<String, String>> tables = ctx.getTables();

        // no sharding table
        String noShardingNode = RouterUtil.isNoSharding(schema, tables.get(0).getValue());
        if (noShardingNode != null) {
            return RouterUtil.routeToSingleNode(rrs, noShardingNode);
        }

        if (tables.size() == 1) {
            return RouterUtil.tryRouteForOneTable(schema, routeUnit, tables.get(0).getValue(), rrs, isSelect);
        }

        /*
         * multi-table it must be ER OR   global* normal , global* er
         */
        //map <table,data_nodes>
        Map<Pair<String, String>, Set<String>> tablesRouteMap = new HashMap<>();

        Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if (tablesAndConditions != null && tablesAndConditions.size() > 0) {
            //findRouter for shard-ing table
            RouterUtil.findRouterForTablesInOneSchema(schema, rrs, tablesAndConditions, tablesRouteMap, false);
            if (rrs.isFinishedRoute()) {
                return rrs;
            }
        }

        //findRouter for singe table global table will not change the result
        // if global table and normal table has no intersection ,they had treat as normal join
        for (Pair<String, String> table : tables) {
            String tableName = table.getValue();
            String testShardingNode = RouterUtil.isNoSharding(schema, tableName);
            if (testShardingNode != null && tablesRouteMap.size() == 0) {
                return RouterUtil.routeToSingleNode(rrs, testShardingNode);
            }
            TableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig != null && !tableConfig.isGlobalTable() && tablesRouteMap.get(table) == null) { //the other is single table
                tablesRouteMap.put(table, new HashSet<>());
                tablesRouteMap.get(table).addAll(tableConfig.getDataNodes());
            }
        }

        Set<String> retNodesSet = retainRouteMap(tablesRouteMap);
        if (retNodesSet.size() == 0 && LOGGER.isTraceEnabled()) {
            LOGGER.trace("this RouteCalculateUnit is always false, so ignore:" + routeUnit);
        }
        routeToMultiNode(isSelect, rrs, retNodesSet);
        return rrs;

    }


    /**
     * tryRouteFor multiTables
     */
    private static RouteResultset tryRouteForTablesComplex(Map<Pair<String, String>, SchemaConfig> schemaMap, DruidShardingParseInfo ctx,
                                                           RouteCalculateUnit routeUnit, RouteResultset rrs)
            throws SQLException {

        List<Pair<String, String>> tables = ctx.getTables();

        Pair<String, String> firstTable = tables.get(0);
        // no sharding table
        String noShardingNode = RouterUtil.isNoSharding(schemaMap.get(firstTable), firstTable.getValue());
        if (noShardingNode != null) {
            return RouterUtil.routeToSingleNode(rrs, noShardingNode);
        }

        if (tables.size() == 1) {
            return RouterUtil.tryRouteForOneTable(schemaMap.get(firstTable), routeUnit, firstTable.getValue(), rrs, true);
        }

        /*
         * multi-table it must be ER OR   global* normal , global* er
         */
        //map <table,data_nodes>
        Map<Pair<String, String>, Set<String>> tablesRouteMap = new HashMap<>();

        Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if (tablesAndConditions != null && tablesAndConditions.size() > 0) {
            //findRouter for shard-ing table
            RouterUtil.findRouterForMultiSchemaTables(schemaMap, rrs, tablesAndConditions, tablesRouteMap);
            if (rrs.isFinishedRoute()) {
                return rrs;
            }
        }

        //findRouter for singe table * global table will not change the result
        // if global table and normal table has no intersection ,they had treat as normal join
        for (Pair<String, String> table : tables) {
            SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(table.getKey());
            String tableName = table.getValue();
            String testShardingNode = RouterUtil.isNoSharding(schema, tableName);
            if (testShardingNode != null && tablesRouteMap.size() == 0) {
                return RouterUtil.routeToSingleNode(rrs, testShardingNode);
            }
            TableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig != null && !tableConfig.isGlobalTable() && tablesRouteMap.get(table) == null) { //the other is single table
                tablesRouteMap.put(table, new HashSet<>());
                tablesRouteMap.get(table).addAll(tableConfig.getDataNodes());
            }
        }

        Set<String> retNodesSet = retainRouteMap(tablesRouteMap);
        if (retNodesSet.size() == 0 && LOGGER.isTraceEnabled()) {
            LOGGER.trace("this RouteCalculateUnit is always false, so ignore:" + routeUnit);
        }
        routeToMultiNode(true, rrs, retNodesSet);
        return rrs;

    }

    private static Set<String> retainRouteMap(Map<Pair<String, String>, Set<String>> tablesRouteMap) throws SQLNonTransientException {
        Set<String> retNodesSet = new HashSet<>();
        boolean isFirstAdd = true;
        for (Map.Entry<Pair<String, String>, Set<String>> entry : tablesRouteMap.entrySet()) {
            if (entry.getValue() == null || entry.getValue().size() == 0) {
                throw new SQLNonTransientException("parent key can't find any valid data node ");
            } else {
                if (isFirstAdd) {
                    retNodesSet.addAll(entry.getValue());
                    isFirstAdd = false;
                } else {
                    retNodesSet.retainAll(entry.getValue());
                    if (retNodesSet.size() == 0) {
                        return retNodesSet;
                    }
                }
            }
        }
        return retNodesSet;
    }


    /**
     * tryRouteForOneTable
     */
    public static RouteResultset tryRouteForOneTable(SchemaConfig schema,
                                                     RouteCalculateUnit routeUnit, String tableName, RouteResultset rrs,
                                                     boolean isSelect) throws SQLException {
        TableConfig tc = schema.getTables().get(tableName);
        if (tc == null) {
            String msg = "Table '" + schema.getName() + "." + tableName + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }

        if (tc.isGlobalTable()) {
            if (isSelect) {
                // global select ,not cache route result
                rrs.setSqlRouteCacheAble(false);
                rrs.setGlobalTable(true);
                String randomDataNode = tc.getRandomDataNode();
                rrs = routeToSingleNode(rrs, randomDataNode);
                List<String> globalBackupNodes = new ArrayList<>(tc.getDataNodes().size() - 1);
                for (String dataNode : tc.getDataNodes()) {
                    if (!dataNode.equals(randomDataNode)) {
                        globalBackupNodes.add(dataNode);
                    }
                }
                rrs.setGlobalBackupNodes(globalBackupNodes);
                return rrs;
            } else { //insert into all global table's node
                return routeToMultiNode(false, rrs, tc.getDataNodes(), true);
            }
        } else { //single table or shard-ing table

            Pair<String, String> table = new Pair<>(schema.getName(), tableName);
            if (!checkRuleRequired(schema, routeUnit, tc, table)) {
                throw new IllegalArgumentException("route rule for table " + schema.getName() + "." +
                        tc.getName() + " is required: " + rrs.getStatement());

            }
            if ((tc.getPartitionColumn() == null && tc.getParentTC() == null) ||
                    (tc.getParentTC() != null && tc.getDirectRouteTC() == null)) {
                // single table or one of the children of complex ER table
                return routeToMultiNode(rrs.isSqlRouteCacheAble(), rrs, tc.getDataNodes());
            } else {
                Map<Pair<String, String>, Set<String>> tablesRouteMap = new HashMap<>();
                if (routeUnit.getTablesAndConditions() != null && routeUnit.getTablesAndConditions().size() > 0) {
                    RouterUtil.findRouterForTablesInOneSchema(schema, rrs, routeUnit.getTablesAndConditions(), tablesRouteMap, true);
                    if (rrs.isFinishedRoute()) {
                        return rrs;
                    }
                }
                if (tablesRouteMap.get(table) == null) {
                    return routeToMultiNode(rrs.isSqlRouteCacheAble(), rrs, tc.getDataNodes());
                } else {
                    return routeToMultiNode(rrs.isSqlRouteCacheAble(), rrs, tablesRouteMap.get(table));
                }
            }
        }
    }

    /**
     * @param schema SchemaConfig
     * @param tc     TableConfig
     * @return true for passed
     */
    private static boolean checkRuleRequired(SchemaConfig schema, RouteCalculateUnit routeUnit, TableConfig tc, Pair<String, String> table) {
        if (!tc.isRuleRequired()) {
            return true;
        }
        boolean hasRequiredValue = false;
        if (routeUnit.getTablesAndConditions().get(table) == null || routeUnit.getTablesAndConditions().get(table).size() == 0) {
            hasRequiredValue = false;
        } else {
            for (Map.Entry<String, ColumnRoute> condition : routeUnit.getTablesAndConditions().get(table).entrySet()) {
                String colName = RouterUtil.getFixedSql(RouterUtil.removeSchema(condition.getKey(), schema.getName()));
                //condition is partition column
                if (colName.equals(tc.getPartitionColumn())) {
                    hasRequiredValue = true;
                    break;
                }
            }
        }
        return hasRequiredValue;
    }


    /**
     * findRouterForMultiSchemaTables
     */
    private static void findRouterForMultiSchemaTables(
            Map<Pair<String, String>, SchemaConfig> schemaMap, RouteResultset rrs,
            Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions,
            Map<Pair<String, String>, Set<String>> tablesRouteMap) throws SQLNonTransientException {

        //router for shard-ing tables
        for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : tablesAndConditions.entrySet()) {
            Pair<String, String> table = entry.getKey();
            String tableName = table.getValue();
            SchemaConfig schema = schemaMap.get(table);
            TableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig != null && !tableConfig.isGlobalTable() && schema.getTables().get(tableName).getDataNodes().size() != 1) {
                //shard-ing table,childTable or others
                if (findRouterWithConditionsForTable(rrs, tablesRouteMap, table, tableConfig, entry.getValue()))
                    return;
            }
        }
    }


    /**
     * findRouterForMultiSchemaTables
     */
    private static void findRouterForTablesInOneSchema(
            SchemaConfig schema, RouteResultset rrs, Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions,
            Map<Pair<String, String>, Set<String>> tablesRouteMap,
            boolean isSingleTable) throws SQLNonTransientException {

        //router for shard-ing tables
        for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : tablesAndConditions.entrySet()) {
            Pair<String, String> table = entry.getKey();
            String tableName = table.getValue();
            TableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig == null) {
                if (isSingleTable) {
                    String msg = " Table '" + schema.getName() + "." + tableName + "' doesn't exist";
                    LOGGER.info(msg);
                    throw new SQLNonTransientException(msg);
                } else {
                    //cross to other schema
                    continue;
                }
            }
            //shard-ing table,childTable or others . global table or single node shard-ing table will router later
            if (!tableConfig.isGlobalTable() && schema.getTables().get(tableName).getDataNodes().size() != 1) {
                if (findRouterWithConditionsForTable(rrs, tablesRouteMap, table, tableConfig, entry.getValue())) return;
            }
        }
    }

    private static boolean findRouterWithConditionsForTable(
            RouteResultset rrs, Map<Pair<String, String>, Set<String>> tablesRouteMap,
            Pair<String, String> table, TableConfig tableConfig,
            Map<String, ColumnRoute> columnsMap) throws SQLNonTransientException {
        String joinKey = tableConfig.getJoinKey();
        String partitionCol = tableConfig.getPartitionColumn();
        boolean isFoundPartitionValue = partitionCol != null && columnsMap.get(partitionCol) != null;

        // where filter contains partition column
        if (isFoundPartitionValue) {
            ColumnRoute partitionValue = columnsMap.get(partitionCol);
            Set<String> dataNodeSet = ruleCalculate(rrs, tableConfig, partitionValue, rrs.isComplexSQL());
            if (dataNodeSet.size() > 0) {
                tablesRouteMap.computeIfAbsent(table, k -> new HashSet<>());
                tablesRouteMap.get(table).addAll(dataNodeSet);
            }
        } else if (joinKey != null && columnsMap.get(joinKey) != null) {
            routerForJoinTable(rrs, tableConfig, columnsMap, joinKey);
            return true;
        } else {
            //no partition column,router to all nodes
            tablesRouteMap.computeIfAbsent(table, k -> new HashSet<>());
            tablesRouteMap.get(table).addAll(tableConfig.getDataNodes());
        }
        return false;
    }


    private static void routerForJoinTable(RouteResultset rrs, TableConfig tableConfig, Map<String, ColumnRoute> columnsMap, String joinKey) throws SQLNonTransientException {
        //childTable  (if it's ER JOIN of select)must find root table,remove childTable, only left root table
        ColumnRoute joinKeyValue = columnsMap.get(joinKey);
        Set<String> dataNodeSet = ruleByJoinValueCalculate(rrs, tableConfig, joinKeyValue);

        if (dataNodeSet.isEmpty()) {
            throw new SQLNonTransientException(
                    "parent key can't find any valid data node ");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("found partition nodes (using parent partition rule directly) for child table to update  " +
                    Arrays.toString(dataNodeSet.toArray()) + " sql :" + rrs.getStatement());
        }
        if (dataNodeSet.size() > 1) {
            routeToMultiNode(rrs.isSqlRouteCacheAble(), rrs, dataNodeSet);
            rrs.setFinishedRoute(true);
        } else {
            rrs.setSqlRouteCacheAble(true);
            routeToSingleNode(rrs, dataNodeSet.iterator().next());
        }
    }


    /**
     * no shard-ing table dataNode
     *
     * @param schemaConfig the SchemaConfig info
     * @param tableName    the TableName
     * @return dataNode DataNode of no-sharding table
     */
    public static String isNoSharding(SchemaConfig schemaConfig, String tableName) throws SQLNonTransientException {
        if (schemaConfig == null || ProxyMeta.getInstance().getTmManager().getSyncView(schemaConfig.getName(), tableName) instanceof QueryNode) {
            return null;
        }
        if (schemaConfig.isNoSharding()) { //schema without table
            return schemaConfig.getDataNode();
        }
        TableConfig tbConfig = schemaConfig.getTables().get(tableName);
        if (tbConfig == null && schemaConfig.getDataNode() != null) {
            return schemaConfig.getDataNode();
        }
        if (tbConfig != null && tbConfig.isNoSharding()) {
            return tbConfig.getDataNodes().get(0);
        }
        return null;
    }

    /**
     * no shard-ing table dataNode
     *
     * @param schemaConfig the SchemaConfig info
     * @param tableName    the TableName
     * @return dataNode DataNode of no-sharding table
     */
    public static String isNoShardingDDL(SchemaConfig schemaConfig, String tableName) {
        if (schemaConfig == null) {
            return null;
        }
        if (schemaConfig.isNoSharding()) { //schema without table
            return schemaConfig.getDataNode();
        }
        TableConfig tbConfig = schemaConfig.getTables().get(tableName);
        if (tbConfig == null && schemaConfig.getDataNode() != null) {
            return schemaConfig.getDataNode();
        }
        if (tbConfig != null && tbConfig.isNoSharding()) {
            return tbConfig.getDataNodes().get(0);
        }
        return null;
    }


    public static boolean isConditionAlwaysTrue(SQLExpr expr) {
        Object o = WallVisitorUtils.getValue(expr);
        return Boolean.TRUE.equals(o);
    }

    public static boolean isConditionAlwaysFalse(SQLExpr expr) {
        Object o = WallVisitorUtils.getValue(expr);
        return Boolean.FALSE.equals(o);
    }


}
