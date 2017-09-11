/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.config.model.rule.RuleConfig;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.function.AbstractPartitionAlgorithm;
import com.actiontech.dble.route.parser.druid.DruidParser;
import com.actiontech.dble.route.parser.druid.DruidShardingParseInfo;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.sqlengine.mpp.ColumnRoutePair;
import com.actiontech.dble.sqlengine.mpp.LoadData;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.wall.spi.WallVisitorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * ServerRouterUtil
 *
 * @author wang.dw
 */
public final class RouterUtil {
    private RouterUtil() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterUtil.class);

    public static String removeSchema(String stmt, String schema) {
        return removeSchema(stmt, schema, DbleServer.getInstance().getConfig().getSystem().isLowerCaseTableNames());
    }

    /**
     * removeSchema from sql
     *
     * @param stmt
     * @param schema      has change to lowercase if need
     * @param isLowerCase lowercase
     * @return new sql
     */
    public static String removeSchema(String stmt, String schema, boolean isLowerCase) {
        final String forCmpStmt = isLowerCase ? stmt.toLowerCase() : stmt;
        final String maySchema1 = schema + ".";
        final String maySchema2 = "`" + schema + "`.";
        int indx1 = forCmpStmt.indexOf(maySchema1, 0);
        int indx2 = forCmpStmt.indexOf(maySchema2, 0);
        if (indx1 < 0 && indx2 < 0) {
            return stmt;
        }
        int strtPos = 0;
        boolean flag;
        int firstE = forCmpStmt.indexOf("'");
        int endE = forCmpStmt.lastIndexOf("'");
        StringBuilder result = new StringBuilder();
        while (indx1 >= 0 || indx2 >= 0) {
            //match `schema` or `schema`
            if (indx1 < 0 && indx2 >= 0) {
                flag = true;
            } else if (indx1 >= 0 && indx2 < 0) {
                flag = false;
            } else flag = indx2 < indx1;
            if (flag) {
                result.append(stmt.substring(strtPos, indx2));
                strtPos = indx2 + maySchema2.length();
                if (indx2 > firstE && indx2 < endE && countChar(stmt, indx2) % 2 != 0) {
                    result.append(stmt.substring(indx2, strtPos));
                }
                indx2 = forCmpStmt.indexOf(maySchema2, strtPos);
            } else {
                result.append(stmt.substring(strtPos, indx1));
                strtPos = indx1 + maySchema1.length();
                if (indx1 > firstE && indx1 < endE && countChar(stmt, indx1) % 2 != 0) {
                    result.append(stmt.substring(indx1, strtPos));
                }
                indx1 = forCmpStmt.indexOf(maySchema1, strtPos);
            }
        }
        result.append(stmt.substring(strtPos));
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

    public static RouteResultset routeFromParser(DruidParser druidParser, SchemaConfig schema, RouteResultset rrs, SQLStatement statement,
                                                 String originSql, LayerCachePool cachePool, ServerSchemaStatVisitor visitor, ServerConnection sc) throws SQLException {
        schema = druidParser.parser(schema, rrs, statement, originSql, cachePool, visitor, sc);
        if (rrs.isFinishedExecute()) {
            return null;
        }
        if (rrs.isFinishedRoute()) {
            return rrs;
        }

        /**
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
            RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, druidParser.getCtx(), unit, rrs, isSelect(statement), cachePool);
            if (rrsTmp != null) {
                Collections.addAll(nodeSet, rrsTmp.getNodes());
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

    public static void routeToSingleDDLNode(SchemaInfo schemaInfo, RouteResultset rrs) throws SQLException {
        rrs.setSchema(schemaInfo.getSchema());
        rrs.setTable(schemaInfo.getTable());
        RouterUtil.routeToSingleNode(rrs, schemaInfo.getSchemaConfig().getDataNode());
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
     * @param rrs
     * @param dataNode
     * @return
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
        stmt = stmt.replaceAll("\r\n", " ");
        return stmt = stmt.trim();
    }

    /**
     * getTableName
     *
     * @param stmt
     * @param repPos
     * @return name
     * @author AStoneGod
     */
    public static String getTableName(String stmt, int[] repPos) {
        int startPos = repPos[0];
        int secInd = stmt.indexOf(' ', startPos + 1);
        if (secInd < 0) {
            secInd = stmt.length();
        }
        int thiInd = stmt.indexOf('(', secInd + 1);
        if (thiInd < 0) {
            thiInd = stmt.length();
        }
        repPos[1] = secInd;
        String tableName = "";
        if (stmt.toUpperCase().startsWith("DESC") || stmt.toUpperCase().startsWith("DESCRIBE")) {
            tableName = stmt.substring(startPos, thiInd).trim();
        } else {
            tableName = stmt.substring(secInd, thiInd).trim();
        }

        //ALTER TABLE
        if (tableName.contains(" ")) {
            tableName = tableName.substring(0, tableName.indexOf(" "));
        }
        int ind2 = tableName.indexOf('.');
        if (ind2 > 0) {
            tableName = tableName.substring(ind2 + 1);
        }
        return lowerCaseTable(tableName);
    }


    public static String lowerCaseTable(String tableName) {
        if (DbleServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
            return tableName.toLowerCase();
        }
        return tableName;
    }


    public static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs, Collection<String> dataNodes) {
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
        rrs.setCacheAble(cache);
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

    /**
     * getRandomDataNode
     *
     * @param schema
     * @param table
     * @return datanode
     * @author mycat
     */
    private static String getRandomDataNode(SchemaConfig schema,
                                            String table) throws SQLException {
        String dataNode = null;
        Map<String, TableConfig> tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(table)) != null) {
            dataNode = tc.getRandomDataNode();
        } else {
            String msg = "Table '" + schema.getName() + "." + table + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }
        return dataNode;
    }

    public static Set<String> ruleByJoinValueCalculate(RouteResultset rrs, TableConfig tc,
                                                       Set<ColumnRoutePair> colRoutePairSet) throws SQLNonTransientException {
        Set<String> retNodeSet = new LinkedHashSet<>();
        if (tc.getDirectRouteTC() != null) {
            Set<String> nodeSet = ruleCalculate(tc.getDirectRouteTC(), colRoutePairSet);
            if (nodeSet.isEmpty()) {
                throw new SQLNonTransientException("parent key can't find  valid datanode ,expect 1 but found: " + nodeSet.size());
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  " + nodeSet + " sql :" + rrs.getStatement());
            }
            retNodeSet.addAll(nodeSet);
            return retNodeSet;
        } else {
            retNodeSet.addAll(tc.getParentTC().getDataNodes());
        }
        return retNodeSet;
    }

    public static Set<String> ruleCalculate(TableConfig tc, Set<ColumnRoutePair> colRoutePairSet) {
        Set<String> routeNodeSet = new LinkedHashSet<>();
        String col = tc.getRule().getColumn();
        RuleConfig rule = tc.getRule();
        AbstractPartitionAlgorithm algorithm = rule.getRuleAlgorithm();
        for (ColumnRoutePair colPair : colRoutePairSet) {
            if (colPair.colValue != null) {
                Integer nodeIndx = algorithm.calculate(colPair.colValue);
                if (nodeIndx == null) {
                    throw new IllegalArgumentException("can't find datanode for sharding column:" + col + " val:" + colPair.colValue);
                } else {
                    String dataNode = tc.getDataNodes().get(nodeIndx);
                    routeNodeSet.add(dataNode);
                    colPair.setNodeId(nodeIndx);
                }
            } else if (colPair.rangeValue != null) {
                Integer[] nodeRange = algorithm.calculateRange(String.valueOf(colPair.rangeValue.getBeginValue()), String.valueOf(colPair.rangeValue.getEndValue()));
                if (nodeRange != null) {
                    /**
                     * not sure  colPair's nodeid has other effect
                     */
                    if (nodeRange.length == 0) {
                        routeNodeSet.addAll(tc.getDataNodes());
                    } else {
                        ArrayList<String> dataNodes = tc.getDataNodes();
                        String dataNode = null;
                        for (Integer nodeId : nodeRange) {
                            dataNode = dataNodes.get(nodeId);
                            routeNodeSet.add(dataNode);
                        }
                    }
                }
            }

        }
        return routeNodeSet;
    }

    /**
     * tryRouteFor multiTables
     */
    public static RouteResultset tryRouteForTables(SchemaConfig schema, DruidShardingParseInfo ctx,
                                                   RouteCalculateUnit routeUnit, RouteResultset rrs, boolean isSelect, LayerCachePool cachePool)
            throws SQLException {

        List<String> tables = ctx.getTables();

        // no sharding table
        if (isNoSharding(schema, tables.get(0))) {
            return routeToSingleNode(rrs, schema.getDataNode());
        }

        if (tables.size() == 1) {
            return RouterUtil.tryRouteForOneTable(schema, routeUnit, tables.get(0), rrs, isSelect, cachePool);
        }

        /**
         * multi-table it must be ER OR   global* normal , global* er
         */
        //map <table,data_nodes>
        Map<String, Set<String>> tablesRouteMap = new HashMap<>();

        Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if (tablesAndConditions != null && tablesAndConditions.size() > 0) {
            //findRouter for shard-ing table
            RouterUtil.findRouterWithConditionsForTables(schema, rrs, tablesAndConditions, tablesRouteMap, cachePool, isSelect, false);
            if (rrs.isFinishedRoute()) {
                return rrs;
            }
        }

        //findRouter for singe table global table will not change the result
        // if global table and normal table has no intersection ,they had treat as normal join
        for (String tableName : tables) {
            TableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig != null && !tableConfig.isGlobalTable() && tablesRouteMap.get(tableName) == null) { //the other is single table
                tablesRouteMap.put(tableName, new HashSet<String>());
                tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
            }
        }

        Set<String> retNodesSet = new HashSet<>();
        boolean isFirstAdd = true;
        for (Map.Entry<String, Set<String>> entry : tablesRouteMap.entrySet()) {
            if (entry.getValue() == null || entry.getValue().size() == 0) {
                throw new SQLNonTransientException("parent key can't find any valid datanode ");
            } else {
                if (isFirstAdd) {
                    retNodesSet.addAll(entry.getValue());
                    isFirstAdd = false;
                } else {
                    retNodesSet.retainAll(entry.getValue());
                    if (retNodesSet.size() == 0) { //two tables has no no intersection
                        String errMsg = "invalid route in sql, multi tables found but datanode has no intersection " +
                                " sql:" + rrs.getStatement();
                        LOGGER.warn(errMsg);
                        throw new SQLNonTransientException(errMsg);
                    }
                }
            }
        }
        //retNodesSet.size() >0
        routeToMultiNode(isSelect, rrs, retNodesSet);
        return rrs;

    }

    /**
     * tryRouteForOneTable
     */
    public static RouteResultset tryRouteForOneTable(SchemaConfig schema,
                                                     RouteCalculateUnit routeUnit, String tableName, RouteResultset rrs, boolean isSelect,
                                                     LayerCachePool cachePool) throws SQLException {
        TableConfig tc = schema.getTables().get(tableName);
        if (tc == null) {
            String msg = "Table '" + schema.getName() + "." + tableName + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }


        if (tc.isGlobalTable()) {
            if (isSelect) {
                // global select ,not cache route result
                rrs.setCacheAble(false);
                rrs.setGlobalTable(true);
                return routeToSingleNode(rrs, tc.getRandomDataNode());
            } else { //insert into all global table's node
                return routeToMultiNode(false, rrs, tc.getDataNodes(), true);
            }
        } else { //single table or shard-ing table
            if (!checkRuleRequired(schema, routeUnit, tc)) {
                throw new IllegalArgumentException("route rule for table " +
                        tc.getName() + " is required: " + rrs.getStatement());

            }
            if ((tc.getPartitionColumn() == null && tc.getParentTC() == null) ||
                    (tc.getParentTC() != null && tc.getDirectRouteTC() == null)) {
                // single table or one of the children of complex ER table
                return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes());
            } else {
                Map<String, Set<String>> tablesRouteMap = new HashMap<>();
                if (routeUnit.getTablesAndConditions() != null && routeUnit.getTablesAndConditions().size() > 0) {
                    RouterUtil.findRouterWithConditionsForTables(schema, rrs, routeUnit.getTablesAndConditions(), tablesRouteMap, cachePool, isSelect, true);
                    if (rrs.isFinishedRoute()) {
                        return rrs;
                    }
                }
                if (tablesRouteMap.get(tableName) == null) {
                    return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes());
                } else {
                    return routeToMultiNode(rrs.isCacheAble(), rrs, tablesRouteMap.get(tableName));
                }
            }
        }
    }


    private static boolean tryRouteWithPrimaryCache(
            RouteResultset rrs, Map<String, Set<String>> tablesRouteMap,
            LayerCachePool cachePool, Map<String, Set<ColumnRoutePair>> columnsMap,
            SchemaConfig schema, String tableName, String primaryKey, boolean isSelect) {
        if (cachePool == null || primaryKey == null || columnsMap.get(primaryKey) == null) {
            return false;
        }
        if (LOGGER.isDebugEnabled() && rrs.getStatement().startsWith(LoadData.LOAD_DATA_HINT) || rrs.isLoadData()) {
            // load data will not cache
            return false;
        }
        //try by primary key if found in cache
        Set<ColumnRoutePair> primaryKeyPairs = columnsMap.get(primaryKey);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("try to find cache by primary key ");
        }

        String tableKey = StringUtil.getFullName(schema.getName(), tableName, '_');
        boolean allFound = true;
        for (ColumnRoutePair pair : primaryKeyPairs) { // may be has multi value of primary key, eg: in(1,2,3)
            String cacheKey = pair.colValue;
            String dataNode = (String) cachePool.get(tableKey, cacheKey);
            if (dataNode == null) {
                allFound = false;
                break;
            } else {
                if (tablesRouteMap.get(tableName) == null) {
                    tablesRouteMap.put(tableName, new HashSet<String>());
                }
                tablesRouteMap.get(tableName).add(dataNode);
            }
        }
        if (!allFound && isSelect) {
            // need cache primary key ->dataNode relation
            rrs.setPrimaryKey(tableKey + '.' + primaryKey);
        }
        return allFound;
    }

    /**
     * findRouterWithConditionsForTables
     */
    public static void findRouterWithConditionsForTables(SchemaConfig schema, RouteResultset rrs,
                                                         Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
                                                         Map<String, Set<String>> tablesRouteMap, LayerCachePool cachePool,
                                                         boolean isSelect, boolean isSingleTable) throws SQLNonTransientException {

        //router for shard-ing tables
        for (Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
            String tableName = entry.getKey();
            if (DbleServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
                tableName = tableName.toLowerCase();
            }
            if (tableName.startsWith(schema.getName() + ".")) {
                tableName = tableName.substring(schema.getName().length() + 1);
            }
            TableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig == null) {
                if (isSingleTable) {
                    String msg = "can't find table [" + tableName + "[ define in schema " + ":" + schema.getName();
                    LOGGER.warn(msg);
                    throw new SQLNonTransientException(msg);
                } else {
                    //cross to other schema
                    continue;
                }
            }
            if (tableConfig.isGlobalTable() || schema.getTables().get(tableName).getDataNodes().size() == 1) {
                //global table or single node shard-ing table will router later
                continue;
            } else { //shard-ing table,childTable or others
                Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
                if (tryRouteWithPrimaryCache(rrs, tablesRouteMap, cachePool, columnsMap, schema, tableName, tableConfig.getPrimaryKey(), isSelect)) {
                    continue;
                }

                String joinKey = tableConfig.getJoinKey();
                String partitionCol = tableConfig.getPartitionColumn();
                boolean isFoundPartitionValue = partitionCol != null && columnsMap.get(partitionCol) != null;

                // where filter contains partition column
                if (isFoundPartitionValue) {
                    Set<ColumnRoutePair> partitionValue = columnsMap.get(partitionCol);
                    if (partitionValue.size() == 0) {
                        if (tablesRouteMap.get(tableName) == null) {
                            tablesRouteMap.put(tableName, new HashSet<String>());
                        }
                        tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
                    } else {
                        routeWithPartition(tablesRouteMap, tableName, tableConfig, partitionValue);
                    }
                } else if (joinKey != null && columnsMap.get(joinKey) != null && columnsMap.get(joinKey).size() != 0) {
                    routerForJoinTable(rrs, tableConfig, columnsMap, joinKey);
                    return;
                } else {
                    //no partition column,router to all nodes
                    if (tablesRouteMap.get(tableName) == null) {
                        tablesRouteMap.put(tableName, new HashSet<String>());
                    }
                    tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
                }
            }
        }
    }

    private static void routeWithPartition(Map<String, Set<String>> tablesRouteMap, String tableName, TableConfig tableConfig, Set<ColumnRoutePair> partitionValue) throws SQLNonTransientException {
        for (ColumnRoutePair pair : partitionValue) {
            AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
            if (pair.colValue != null) {
                Integer nodeIndex = algorithm.calculate(pair.colValue);
                if (nodeIndex == null) {
                    String msg = "can't find any valid datanode :" + tableConfig.getName() +
                            " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
                    LOGGER.warn(msg);
                    throw new SQLNonTransientException(msg);
                }

                ArrayList<String> dataNodes = tableConfig.getDataNodes();
                String node;
                if (nodeIndex >= 0 && nodeIndex < dataNodes.size()) {
                    node = dataNodes.get(nodeIndex);

                } else {
                    node = null;
                    String msg = "Can't find a valid data node for specified node index :" +
                            tableConfig.getName() + " -> " + tableConfig.getPartitionColumn() +
                            " -> " + pair.colValue + " -> " + "Index : " + nodeIndex;
                    LOGGER.warn(msg);
                    throw new SQLNonTransientException(msg);
                }
                if (node != null) {
                    if (tablesRouteMap.get(tableName) == null) {
                        tablesRouteMap.put(tableName, new HashSet<String>());
                    }
                    tablesRouteMap.get(tableName).add(node);
                }
            }
            if (pair.rangeValue != null) {
                Integer[] nodeIndexs = algorithm.calculateRange(
                        pair.rangeValue.getBeginValue().toString(), pair.rangeValue.getEndValue().toString());
                ArrayList<String> dataNodes = tableConfig.getDataNodes();
                String node;
                for (Integer idx : nodeIndexs) {
                    if (idx >= 0 && idx < dataNodes.size()) {
                        node = dataNodes.get(idx);
                    } else {
                        String msg = "Can't find valid data node(s) for some of specified node indexes :" +
                                tableConfig.getName() + " -> " + tableConfig.getPartitionColumn();
                        LOGGER.warn(msg);
                        throw new SQLNonTransientException(msg);
                    }
                    if (node != null) {
                        if (tablesRouteMap.get(tableName) == null) {
                            tablesRouteMap.put(tableName, new HashSet<String>());
                        }
                        tablesRouteMap.get(tableName).add(node);
                    }
                }
            }
        }
    }

    private static void routerForJoinTable(RouteResultset rrs, TableConfig tableConfig, Map<String, Set<ColumnRoutePair>> columnsMap, String joinKey) throws SQLNonTransientException {
        //childTable  (if it's ER JOIN of select)must find root table,remove childTable, only left root table
        Set<ColumnRoutePair> joinKeyValue = columnsMap.get(joinKey);
        Set<String> dataNodeSet = ruleByJoinValueCalculate(rrs, tableConfig, joinKeyValue);

        if (dataNodeSet.isEmpty()) {
            throw new SQLNonTransientException(
                    "parent key can't find any valid datanode ");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("found partion nodes (using parent partion rule directly) for child table to update  " +
                    Arrays.toString(dataNodeSet.toArray()) + " sql :" + rrs.getStatement());
        }
        if (dataNodeSet.size() > 1) {
            routeToMultiNode(rrs.isCacheAble(), rrs, dataNodeSet);
            rrs.setFinishedRoute(true);
            return;
        } else {
            rrs.setCacheAble(true);
            routeToSingleNode(rrs, dataNodeSet.iterator().next());
            return;
        }
    }

    /**
     * @param schema
     * @param tc
     * @return true for passed
     */
    public static boolean checkRuleRequired(SchemaConfig schema, RouteCalculateUnit routeUnit, TableConfig tc) {
        if (!tc.isRuleRequired()) {
            return true;
        }
        boolean hasRequiredValue = false;
        String tableName = tc.getName();
        if (routeUnit.getTablesAndConditions().get(tableName) == null || routeUnit.getTablesAndConditions().get(tableName).size() == 0) {
            hasRequiredValue = false;
        } else {
            for (Map.Entry<String, Set<ColumnRoutePair>> condition : routeUnit.getTablesAndConditions().get(tableName).entrySet()) {

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
     * no shard-ing table dataNode
     *
     * @param schemaConfig
     * @param tableName
     * @return
     */
    public static boolean isNoSharding(SchemaConfig schemaConfig, String tableName) {
        if (schemaConfig == null) {
            return false;
        }
        if (schemaConfig.isNoSharding()) {
            return true;
        }
        return schemaConfig.getDataNode() != null && !schemaConfig.getTables().containsKey(tableName);
    }

    /**
     * isConditionAlwaysTrue
     *
     * @param expr
     * @return
     */
    public static boolean isConditionAlwaysTrue(SQLExpr expr) {
        Object o = WallVisitorUtils.getValue(expr);
        return Boolean.TRUE.equals(o);
    }

    /**
     * isConditionAlwaysFalse
     *
     * @param expr
     * @return
     */
    public static boolean isConditionAlwaysFalse(SQLExpr expr) {
        Object o = WallVisitorUtils.getValue(expr);
        return Boolean.FALSE.equals(o);
    }
}
