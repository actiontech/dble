/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.datasource.PhysicalDataSource;
import com.actiontech.dble.cache.LayerCachePool;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.config.model.rule.RuleConfig;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.QueryNode;
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
import com.actiontech.dble.singleton.CacheService;
import com.actiontech.dble.singleton.ProxyMeta;
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
     * @param stmt
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
            } else flag = index2 < index1;
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


    public static RouteResultset routeFromParserComplex(DruidParser druidParser, Map<String, SchemaConfig> schemaMap, RouteResultset rrs, SQLStatement statement,
                                                        String originSql, LayerCachePool cachePool, ServerSchemaStatVisitor visitor,
                                                        ServerConnection sc, PlanNode node) throws SQLException {
        druidParser.parser(null, rrs, statement, originSql, cachePool, visitor, sc);
        if (rrs.isFinishedExecute()) {
            return null;
        }
        if (rrs.isFinishedRoute()) {
            return rrs;
        }

        /* multi-tables*/
        if (druidParser.getCtx().getRouteCalculateUnits().size() == 0) {
            RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
            druidParser.getCtx().addRouteCalculateUnit(routeCalculateUnit);
        }

        SortedSet<RouteResultsetNode> nodeSet = new TreeSet<>();
        for (RouteCalculateUnit unit : druidParser.getCtx().getRouteCalculateUnits()) {
            RouteResultset rrsTmp = RouterUtil.tryRouteForTablesComplex(schemaMap, druidParser.getCtx(), unit, rrs, isSelect(statement), cachePool, node);
            if (rrsTmp != null && rrsTmp.getNodes() != null) {
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
                                                 String originSql, LayerCachePool cachePool, ServerSchemaStatVisitor visitor,
                                                 ServerConnection sc, PlanNode node, boolean isExplain) throws SQLException {
        schema = druidParser.parser(schema, rrs, statement, originSql, cachePool, visitor, sc, isExplain);
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
            RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, druidParser.getCtx(), unit, rrs, isSelect(statement), cachePool, node);
            if (rrsTmp != null && rrsTmp.getNodes() != null) {
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
        return stmt.replaceAll("[\\t\\n\\r]", " ").trim();
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
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
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

    public static String getRandomDataNode(ArrayList<String> dataNodes) {
        int index = Math.abs(rand.nextInt(Integer.MAX_VALUE)) % dataNodes.size();
        ArrayList<String> x = new ArrayList<>(dataNodes);
        Map<String, PhysicalDataNode> dataNodeMap = DbleServer.getInstance().getConfig().getDataNodes();
        while (x.size() > 1) {
            for (PhysicalDataSource ds : dataNodeMap.get(x.get(index)).getDataHost().getAllActiveDataSources()) {
                if (ds.isAlive()) {
                    return x.get(index);
                } else {
                    break;
                }
            }
            x.remove(index);
            index = Math.abs(rand.nextInt(Integer.MAX_VALUE)) % x.size();
        }

        return x.get(0);
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

    public static Set<String> ruleByJoinValueCalculate(String sql, TableConfig tc,
                                                       Set<ColumnRoutePair> colRoutePairSet) throws SQLNonTransientException {
        Set<String> retNodeSet = new LinkedHashSet<>();
        if (tc.getDirectRouteTC() != null) {
            Set<String> nodeSet = ruleCalculate(tc.getDirectRouteTC(), colRoutePairSet);
            if (nodeSet.isEmpty()) {
                throw new SQLNonTransientException("parent key can't find  valid datanode ,expect 1 but found: " + nodeSet.size());
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  " + nodeSet + " sql :" + sql);
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
                Integer nodeIndex = algorithm.calculate(colPair.colValue);
                if (nodeIndex == null) {
                    throw new IllegalArgumentException("can't find datanode for sharding column:" + col + " val:" + colPair.colValue);
                } else {
                    String dataNode = tc.getDataNodes().get(nodeIndex);
                    routeNodeSet.add(dataNode);
                    colPair.setNodeId(nodeIndex);
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
                        String dataNode;
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

    public static String tryRouteTablesToOneNode(String user, RouteResultset rrs, SchemaConfig schemaConfig, DruidShardingParseInfo ctx, Set<String> schemaList, int tableSize, boolean isSelect) throws SQLException {
        if (ctx.getTables().size() != tableSize) {
            return null;
        }
        Set<String> resultNodes = new HashSet<>();
        for (RouteCalculateUnit routeUnit : ctx.getRouteCalculateUnits()) {
            Set<String> tmpResultNodes = new HashSet<>();
            Set<TableConfig> globalTables = new HashSet<>();
            Set<String> tablesSet = new HashSet<>(ctx.getTables());
            Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit.getTablesAndConditions();
            if (tablesAndConditions != null) {
                for (Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
                    String fullTable = entry.getKey();
                    SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(user, schemaConfig, fullTable);
                    SchemaConfig schema = schemaInfo.getSchemaConfig();
                    String schemaName = schemaInfo.getSchema();
                    String tableName = schemaInfo.getTable();
                    schemaList.add(schemaName);
                    TableConfig tableConfig = schema.getTables().get(tableName);
                    if (tableConfig == null) {
                        if (schema.getDataNode() == null) {
                            String msg = " Table '" + schemaName + "." + tableName + "' doesn't exist";
                            LOGGER.info(msg);
                            throw new SQLNonTransientException(msg);
                        } else {
                            tmpResultNodes.add(schema.getDataNode());
                            tablesSet.remove(tableName);
                            if (tmpResultNodes.size() != 1) {
                                return null;
                            }
                        }
                    } else if (tableConfig.isGlobalTable()) {
                        globalTables.add(tableConfig);
                    } else if (schema.getTables().get(tableName).getDataNodes().size() == 1) {
                        tmpResultNodes.add(schema.getTables().get(tableName).getDataNodes().get(0));
                        tablesSet.remove(tableName);
                        if (tmpResultNodes.size() != 1) {
                            return null;
                        }
                    } else {
                        if (!tryCalcNodeForShardingTable(rrs, tmpResultNodes, tablesSet, entry, tableName, schema, tableConfig, isSelect)) {
                            return null;
                        }
                    }
                }
            }
            if (globalTables.size() == tableSize) {
                boolean isFirstTable = true;
                for (TableConfig tb : globalTables) {
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
            for (TableConfig tb : globalTables) {
                tmpResultNodes.retainAll(tb.getDataNodes());
                tablesSet.remove(tb.getName());
            }
            if (tmpResultNodes.size() != 1 || tablesSet.size() != 0) {
                return null;
            }
            resultNodes.add(tmpResultNodes.iterator().next());
        }

        if (resultNodes.size() != 1) {
            return null;
        }
        return resultNodes.iterator().next();
    }

    private static boolean tryCalcNodeForShardingTable(RouteResultset rrs, Set<String> resultNodes, Set<String> tablesSet, Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry, String tableName, SchemaConfig schema, TableConfig tableConfig, boolean isSelect) throws SQLNonTransientException {
        Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();

        Map<String, Set<String>> tablesRouteMap = new HashMap<>();
        if (tryRouteWithCache(rrs, tablesRouteMap, CacheService.getTableId2DataNodeCache(), columnsMap, schema, tableName, tableConfig.getCacheKey(), isSelect)) {
            Set<String> nodes = tablesRouteMap.get(tableName);
            if (nodes == null || nodes.size() != 1) {
                return false;
            } else {
                resultNodes.add(nodes.iterator().next());
                tablesSet.remove(tableName);
                return true;
            }
        }

        String joinKey = tableConfig.getJoinKey();
        String partitionCol = tableConfig.getPartitionColumn();

        // where filter contains partition column
        if (partitionCol != null && columnsMap.get(partitionCol) != null) {
            Set<ColumnRoutePair> partitionValue = columnsMap.get(partitionCol);
            if (partitionValue.size() == 0) {
                return false;
            } else {
                for (ColumnRoutePair pair : partitionValue) {
                    AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
                    if (pair.colValue != null && !"null".equals(pair.colValue)) {
                        Integer nodeIndex;
                        try {
                            nodeIndex = algorithm.calculate(pair.colValue);
                        } catch (Exception e) {
                            return false;
                        }
                        if (nodeIndex == null) {
                            String msg = "can't find any valid data node :" + tableConfig.getName() +
                                    " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
                            LOGGER.info(msg);
                            throw new SQLNonTransientException(msg);
                        }

                        ArrayList<String> dataNodes = tableConfig.getDataNodes();
                        String node;
                        if (nodeIndex >= 0 && nodeIndex < dataNodes.size()) {
                            node = dataNodes.get(nodeIndex);
                        } else {
                            String msg = "Can't find a valid data node for specified node index :" +
                                    tableConfig.getName() + " -> " + tableConfig.getPartitionColumn() +
                                    " -> " + pair.colValue + " -> " + "Index : " + nodeIndex;
                            LOGGER.info(msg);
                            throw new SQLNonTransientException(msg);
                        }
                        if (node != null) {
                            resultNodes.add(node);
                            tablesSet.remove(tableName);
                            if (resultNodes.size() != 1) {
                                return false;
                            }
                        }
                    }
                }
            }
        } else if (joinKey != null && columnsMap.get(joinKey) != null && columnsMap.get(joinKey).size() != 0) {
            Set<ColumnRoutePair> joinKeyValue = columnsMap.get(joinKey);
            Set<String> dataNodeSet = ruleByJoinValueCalculate(rrs.getStatement(), tableConfig, joinKeyValue);
            if (dataNodeSet.size() > 1) {
                return false;
            }
            resultNodes.addAll(dataNodeSet);
            tablesSet.remove(tableName);
            return resultNodes.size() == 1;
        } else {
            return false;
        }
        return true;
    }

    /**
     * tryRouteFor multiTables
     */
    public static RouteResultset tryRouteForTables(SchemaConfig schema, DruidShardingParseInfo ctx,
                                                   RouteCalculateUnit routeUnit, RouteResultset rrs, boolean isSelect, LayerCachePool cachePool, PlanNode node)
            throws SQLException {

        List<String> tables = ctx.getTables();

        // no sharding table
        String noShardingNode = RouterUtil.isNoSharding(schema, tables.get(0));
        if (noShardingNode != null) {
            return RouterUtil.routeToSingleNode(rrs, noShardingNode);
        }

        if (tables.size() == 1) {
            return RouterUtil.tryRouteForOneTable(schema, routeUnit, tables.get(0), rrs, isSelect, cachePool, node);
        }

        /**
         * multi-table it must be ER OR   global* normal , global* er
         */
        //map <table,data_nodes>
        Map<String, Set<String>> tablesRouteMap = new HashMap<>();

        Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if (tablesAndConditions != null && tablesAndConditions.size() > 0) {
            //findRouter for shard-ing table
            RouterUtil.findRouterWithConditionsForTables(schema, rrs, tablesAndConditions, tablesRouteMap, cachePool, isSelect, false, node);
            if (rrs.isFinishedRoute()) {
                return rrs;
            }
        }

        //findRouter for singe table global table will not change the result
        // if global table and normal table has no intersection ,they had treat as normal join
        for (String tableName : tables) {
            String testShardingNode = RouterUtil.isNoSharding(schema, tableName);
            if (testShardingNode != null && tablesRouteMap.size() == 0) {
                return RouterUtil.routeToSingleNode(rrs, testShardingNode);
            }
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
                        LOGGER.info(errMsg);
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
     * tryRouteFor multiTables
     */
    public static RouteResultset tryRouteForTablesComplex(Map<String, SchemaConfig> schemaMap, DruidShardingParseInfo ctx,
                                                          RouteCalculateUnit routeUnit, RouteResultset rrs, boolean isSelect, LayerCachePool cachePool, PlanNode node)
            throws SQLException {

        List<String> tables = ctx.getTables();

        // no sharding table
        String noShardingNode = RouterUtil.isNoSharding(schemaMap.get(tables.get(0)), tables.get(0));
        if (noShardingNode != null) {
            return RouterUtil.routeToSingleNode(rrs, noShardingNode);
        }

        if (tables.size() == 1) {
            return RouterUtil.tryRouteForOneTable((schemaMap.get(tables.get(0))), routeUnit, tables.get(0), rrs, isSelect, cachePool, node);
        }

        /**
         * multi-table it must be ER OR   global* normal , global* er
         */
        //map <table,data_nodes>
        Map<String, Set<String>> tablesRouteMap = new HashMap<>();

        Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if (tablesAndConditions != null && tablesAndConditions.size() > 0) {
            //findRouter for shard-ing table
            RouterUtil.findRouterWithConditionsForTables(schemaMap, rrs, tablesAndConditions, tablesRouteMap, cachePool, isSelect, false, node);
            if (rrs.isFinishedRoute()) {
                return rrs;
            }
        }

        //findRouter for singe table global table will not change the result
        // if global table and normal table has no intersection ,they had treat as normal join
        for (String tableName : tables) {
            SchemaConfig schema = schemaMap.get(tableName);
            String testShardingNode = RouterUtil.isNoSharding(schema, tableName);
            if (testShardingNode != null && tablesRouteMap.size() == 0) {
                return RouterUtil.routeToSingleNode(rrs, testShardingNode);
            }
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
                        LOGGER.info(errMsg);
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
                                                     LayerCachePool cachePool, PlanNode node) throws SQLException {
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
                    RouterUtil.findRouterWithConditionsForTables(schema, rrs, routeUnit.getTablesAndConditions(), tablesRouteMap, cachePool, isSelect, true, node);
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


    private static boolean tryRouteWithCache(
            RouteResultset rrs, Map<String, Set<String>> tablesRouteMap,
            LayerCachePool cachePool, Map<String, Set<ColumnRoutePair>> columnsMap,
            SchemaConfig schema, String tableName, String cacheKey, boolean isSelect) {
        if (cachePool == null || cacheKey == null || columnsMap.get(cacheKey) == null) {
            return false;
        }
        if (LOGGER.isDebugEnabled() && rrs.getStatement().startsWith(LoadData.LOAD_DATA_HINT) || rrs.isLoadData()) {
            // load data will not cache
            return false;
        }
        //try by primary key if found in cache
        Set<ColumnRoutePair> cacheKeyPairs = columnsMap.get(cacheKey);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("try to find cache by primary key ");
        }

        String tableKey = StringUtil.getFullName(schema.getName(), tableName, '_');
        boolean allFound = true;
        for (ColumnRoutePair pair : cacheKeyPairs) { // may be has multi value of primary key, eg: in(1,2,3)
            String cacheValue = pair.colValue;
            String dataNode = (String) cachePool.get(tableKey, cacheValue);
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
            rrs.setContainsPrimaryFilter(true);
        }
        return allFound;
    }


    /**
     * findRouterWithConditionsForTables
     */
    public static void findRouterWithConditionsForTables(Map<String, SchemaConfig> schemaMap, RouteResultset rrs,
                                                         Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
                                                         Map<String, Set<String>> tablesRouteMap, LayerCachePool cachePool,
                                                         boolean isSelect, boolean isSingleTable, PlanNode node) throws SQLNonTransientException {

        //router for shard-ing tables
        for (Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
            String tableName = entry.getKey();
            SchemaConfig schema = schemaMap.get(tableName);
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                tableName = tableName.toLowerCase();
            }
            if (tableName.startsWith(schema.getName() + ".")) {
                tableName = tableName.substring(schema.getName().length() + 1);
            }
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
            if (tableConfig.isGlobalTable() || schema.getTables().get(tableName).getDataNodes().size() == 1) {
                //global table or single node shard-ing table will router later
                continue;
            } else { //shard-ing table,childTable or others
                Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
                if (tryRouteWithCache(rrs, tablesRouteMap, cachePool, columnsMap, schema, tableName, tableConfig.getCacheKey(), isSelect)) {
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
                        routeWithPartition(tablesRouteMap, tableName, tableConfig, partitionValue, node);
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


    /**
     * findRouterWithConditionsForTables
     */
    public static void findRouterWithConditionsForTables(SchemaConfig schema, RouteResultset rrs,
                                                         Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
                                                         Map<String, Set<String>> tablesRouteMap, LayerCachePool cachePool,
                                                         boolean isSelect, boolean isSingleTable, PlanNode node) throws SQLNonTransientException {

        //router for shard-ing tables
        for (Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
            String tableName = entry.getKey();
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                tableName = tableName.toLowerCase();
            }
            if (tableName.startsWith(schema.getName() + ".")) {
                tableName = tableName.substring(schema.getName().length() + 1);
            }
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
            if (tableConfig.isGlobalTable() || schema.getTables().get(tableName).getDataNodes().size() == 1) {
                //global table or single node shard-ing table will router later
                continue;
            } else { //shard-ing table,childTable or others
                Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
                if (tryRouteWithCache(rrs, tablesRouteMap, cachePool, columnsMap, schema, tableName, tableConfig.getCacheKey(), isSelect)) {
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
                        routeWithPartition(tablesRouteMap, tableName, tableConfig, partitionValue, node);
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

    private static void routeWithPartition(Map<String, Set<String>> tablesRouteMap, String tableName, TableConfig tableConfig, Set<ColumnRoutePair> partitionValue, PlanNode pnode) throws SQLNonTransientException {
        for (ColumnRoutePair pair : partitionValue) {
            AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
            if (pnode != null && "null".equals(pair.colValue)) {
                continue;
            } else if (pair.colValue != null) {
                // for explain
                if (NEED_REPLACE.equals(pair.colValue) || ALL_SUB_QUERY_RESULTS.equals(pair.colValue) ||
                        MIN_SUB_QUERY_RESULTS.equals(pair.colValue) || MAX_SUB_QUERY_RESULTS.equals(pair.colValue)) {
                    return;
                }
                Integer nodeIndex = algorithm.calculate(pair.colValue);
                if (nodeIndex == null) {
                    String msg = "can't find any valid data node :" + tableConfig.getName() +
                            " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
                    LOGGER.info(msg);
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
                    LOGGER.info(msg);
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
                Integer[] nodeIndexes = algorithm.calculateRange(
                        pair.rangeValue.getBeginValue().toString(), pair.rangeValue.getEndValue().toString());
                ArrayList<String> dataNodes = tableConfig.getDataNodes();
                String node;
                for (Integer idx : nodeIndexes) {
                    if (idx >= 0 && idx < dataNodes.size()) {
                        node = dataNodes.get(idx);
                    } else {
                        String msg = "Can't find valid data node(s) for some of specified node indexes :" +
                                tableConfig.getName() + " -> " + tableConfig.getPartitionColumn();
                        LOGGER.info(msg);
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
        Set<String> dataNodeSet = ruleByJoinValueCalculate(rrs.getStatement(), tableConfig, joinKeyValue);

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
