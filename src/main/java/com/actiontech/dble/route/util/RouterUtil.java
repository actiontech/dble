/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.subquery.UpdateSubQueryHandler;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.*;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.plan.common.field.FieldUtil;
import com.actiontech.dble.plan.node.QueryNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.DruidParser;
import com.actiontech.dble.route.parser.druid.DruidShardingParseInfo;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.druid.impl.DruidSingleUnitSelectParser;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.server.util.SchemaUtil.SchemaInfo;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.sqlengine.mpp.ColumnRoute;
import com.actiontech.dble.sqlengine.mpp.RangeValue;
import com.actiontech.dble.util.CharsetContext;
import com.actiontech.dble.util.HexFormatUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.wall.spi.WallVisitorUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
    private static final String DDL_TRACE_LOG = "DDL_TRACE";
    private static final Logger DTRACE_LOGGER = LoggerFactory.getLogger(DDL_TRACE_LOG);

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

        //backtracking check
        if (!checkPrefix(forCmpStmt, index1)) {
            index1 = -1;
        }
        if (!checkPrefix(forCmpStmt, index2)) {
            index2 = -1;
        }

        int startPos = 0;
        boolean flag;
        int firstE = forCmpStmt.indexOf("'");
        int endE = forCmpStmt.lastIndexOf("'");
        StringBuilder result = new StringBuilder();
        while (index1 >= 0 || index2 >= 0) {
            //match `sharding` or `sharding`
            if (index1 < 0) {
                flag = true;
            } else if (index2 < 0) {
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
                index2 = recursionIndexOf(forCmpStmt, maySchema2, startPos);
            } else {
                result.append(stmt, startPos, index1);
                startPos = index1 + maySchema1.length();
                if (index1 > firstE && index1 < endE && countChar(stmt, index1) % 2 != 0) {
                    result.append(stmt, index1, startPos);
                }
                index1 = recursionIndexOf(forCmpStmt, maySchema1, startPos);
            }
        }
        result.append(stmt.substring(startPos));
        return result.toString();
    }

    private static int recursionIndexOf(String forCmpStmt, String schema, int startPos) {
        int indexTmp = forCmpStmt.indexOf(schema, startPos);
        if (indexTmp == -1) {
            return -1;
        }
        if (checkPrefix(forCmpStmt, indexTmp)) {
            return indexTmp;
        } else {
            return recursionIndexOf(forCmpStmt, schema, indexTmp + schema.length());
        }
    }

    /**
     * backtracking check
     *
     * @param content
     * @param index
     * @return
     */
    private static boolean checkPrefix(String content, int index) {
        if (index > 0) {
            char prefix = content.charAt(index - 1);
            return !Character.isLetterOrDigit(prefix) && prefix != '_' && prefix != '-' && prefix != '$' && prefix != '.';
        }
        return false;
    }


    private static int countChar(String sql, int end) {
        int count = 0;
        boolean skipChar = false;
        for (int i = 0; i < end; i++) {
            if (sql.charAt(i) == '\'' && !skipChar) {
                count++;
            } else skipChar = sql.charAt(i) == '\\';
        }
        return count;
    }


    public static RouteResultset routeFromParserComplex(
            SchemaConfig schema, DruidParser druidParser, Map<Pair<String, String>, SchemaConfig> schemaMap,
            RouteResultset rrs, SQLStatement statement,
            ServerSchemaStatVisitor visitor, ShardingService service) throws SQLException {
        druidParser.parser(schema, rrs, statement, visitor, service);
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
            RouteResultset rrsTmp = RouterUtil.tryRouteForTablesComplex(schemaMap, druidParser.getCtx(), unit, rrs, service.getCharset().getClient());
            if (rrsTmp != null && rrsTmp.getNodes() != null && rrsTmp.getNodes().length != 0) {
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

        if (rrs.getSqlType() == ServerParse.SELECT) {
            ((DruidSingleUnitSelectParser) druidParser).tryRouteToApNode(schema, rrs, statement, service);
        }
        return rrs;
    }

    public static RouteResultset routeToApNode(RouteResultset rrs, String apNode, Set<String> tableSet) {
        if (apNode == null) {
            return rrs;
        }
        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(apNode, rrs.getSqlType(), rrs.getStatement(), tableSet);
        nodes[0].setApNode(true);
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
        return rrs;
    }


    public static RouteResultset routeFromParser(DruidParser druidParser, SchemaConfig schema, RouteResultset rrs, SQLStatement statement,
                                                 ServerSchemaStatVisitor visitor,
                                                 ShardingService service, boolean isExplain) throws SQLException {
        schema = druidParser.parser(schema, rrs, statement, visitor, service, isExplain);
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
            return RouterUtil.routeToSingleNode(rrs, schema.getRandomShardingNode(), null);
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
            RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, druidParser.getCtx(), unit, rrs, isSelect(statement), service.getCharset().getClient());
            if (rrsTmp != null && rrsTmp.getNodes() != null && rrsTmp.getNodes().length != 0) {
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

    public static void routeToSingleDDLNode(SchemaInfo schemaInfo, RouteResultset rrs, String shardingNode) throws SQLException {
        rrs.setSchema(schemaInfo.getSchema());
        rrs.setTable(schemaInfo.getTable());
        RouterUtil.routeToSingleNode(rrs, shardingNode, Sets.newHashSet(schemaInfo.getSchema() + "." + schemaInfo.getTable()));
    }

    public static boolean tryRouteToSingleDDLNode(SchemaInfo schemaInfo, RouteResultset rrs, String tableName) throws SQLException {
        String shardingNode = isNoShardingDDL(schemaInfo.getSchemaConfig(), tableName);
        if (shardingNode == null)
            return false;
        routeToSingleDDLNode(schemaInfo, rrs, shardingNode);
        return true;
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
        rrs = RouterUtil.routeToSingleNode(rrs, schema.getMetaShardingNode(), null);
        rrs.setFinishedRoute(true);
    }

    public static RouteResultset routeToSingleNode(RouteResultset rrs, String shardingNode, Set<String> tableSet) {
        if (shardingNode == null) {
            return rrs;
        }
        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(shardingNode, rrs.getSqlType(), rrs.getStatement(), tableSet);
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
        if (rrs.getCanRunInReadDB() != null) {
            nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
        }
        if (rrs.getRunOnSlave() != null) {
            nodes[0].setRunOnSlave(rrs.getRunOnSlave());
        }
        if (rrs.isForUpdate()) {
            nodes[0].setForUpdate(true);
        }
        return rrs;
    }


    public static void routeToDDLNode(SchemaInfo schemaInfo, RouteResultset rrs) throws SQLException {
        List<String> shardingNodes;
        Map<String, BaseTableConfig> tables = schemaInfo.getSchemaConfig().getTables();
        BaseTableConfig tc = tables.get(schemaInfo.getTable());
        if (tc != null) {
            shardingNodes = tc.getShardingNodes();
        } else {
            if (schemaInfo.getSchemaConfig().getDefaultShardingNodes() != null && schemaInfo.getSchemaConfig().getDefaultShardingNodes().size() > 1) {
                shardingNodes = schemaInfo.getSchemaConfig().getDefaultShardingNodes();
            } else {
                String msg = "Table '" + schemaInfo.getSchema() + "." + schemaInfo.getTable() + "' doesn't exist";
                throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
            }
        }
        Iterator<String> iterator1 = shardingNodes.iterator();
        int nodeSize = shardingNodes.size();
        String stmt = getFixedSql(removeSchema(rrs.getStatement(), schemaInfo.getSchema()));

        RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSize];
        for (int i = 0; i < nodeSize; i++) {
            String name = iterator1.next();
            nodes[i] = new RouteResultsetNode(name, ServerParse.DDL, stmt, Sets.newHashSet(schemaInfo.getSchema() + "." + schemaInfo.getTable()));
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


    private static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs, Collection<String> shardingNodes, Set<String> tableSet) {
        RouteResultsetNode[] nodes = new RouteResultsetNode[shardingNodes.size()];
        int i = 0;
        for (String shardingNode : shardingNodes) {
            nodes[i] = new RouteResultsetNode(shardingNode, rrs.getSqlType(), rrs.getStatement(), tableSet);
            if (rrs.getCanRunInReadDB() != null) {
                nodes[i].setCanRunInReadDB(rrs.getCanRunInReadDB());
            }
            if (rrs.getRunOnSlave() != null) {
                nodes[i].setRunOnSlave(rrs.getRunOnSlave());
            }
            if (rrs.isForUpdate()) {
                nodes[i].setForUpdate(true);
            }
            i++;
        }
        rrs.setSqlRouteCacheAble(cache);
        rrs.setNodes(nodes);
        return rrs;
    }

    public static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs, Collection<String> shardingNodes, boolean isGlobalTable,
                                                  List<Pair<String, String>> tableList) {
        HashSet<String> tableSet = Sets.newHashSet();
        for (Pair<String, String> table : tableList) {
            tableSet.add(table.getKey() + "." + table.getValue());
        }
        return routeToMultiNode(cache, rrs, shardingNodes, isGlobalTable, tableSet);
    }

    public static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs, Collection<String> shardingNodes, boolean isGlobalTable,
                                                  Set<String> tableSet) {
        routeToMultiNode(cache, rrs, shardingNodes, tableSet);
        rrs.setGlobalTable(isGlobalTable);
        return rrs;
    }

    public static void routeToRandomNode(RouteResultset rrs,
                                         SchemaConfig schema, String tableName) throws SQLException {
        String shardingNode = getRandomShardingNode(schema, tableName);
        routeToSingleNode(rrs, shardingNode, Sets.newHashSet(schema.getName() + "." + tableName));
    }

    public static String getRandomShardingNode(Collection<String> shardingNodes) {
        int index = rand.nextInt(shardingNodes.size());
        ArrayList<String> x = new ArrayList<>(shardingNodes);
        Map<String, ShardingNode> shardingNodeMap = DbleServer.getInstance().getConfig().getShardingNodes();
        while (x.size() > 1) {
            if (shardingNodeMap.get(x.get(index)).getDbGroup().getWriteDbInstance().isAlive()) {
                return x.get(index);
            }
            x.remove(index);
            index = rand.nextInt(x.size());
        }

        return x.get(0);
    }

    private static String getRandomShardingNode(SchemaConfig schema, String table) throws SQLException {
        Map<String, BaseTableConfig> tables = schema.getTables();
        BaseTableConfig tc;
        if (tables != null && (tc = tables.get(table)) != null) {
            return RouterUtil.getRandomShardingNode(tc.getShardingNodes());
        } else {
            String msg = "Table '" + schema.getName() + "." + table + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }
    }

    private static Set<String> ruleByJoinValueCalculate(String schemaName, RouteResultset rrs, ChildTableConfig tc,
                                                        ColumnRoute colRoutePairSet, String clientCharset) throws SQLNonTransientException {
        Set<String> retNodeSet = new LinkedHashSet<>();
        if (tc.getDirectRouteTC() != null) {
            Set<String> nodeSet = ruleCalculate(schemaName, rrs, tc.getDirectRouteTC(), colRoutePairSet, false, clientCharset);
            if (nodeSet.isEmpty()) {
                throw new SQLNonTransientException("parent key can't find  valid shardingNode ,expect 1 but found: 0");
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found partition node (using parent partition rule directly) for child table to insert  " + nodeSet + " sql :" + rrs.getStatement());
            }
            retNodeSet.addAll(nodeSet);
            return retNodeSet;
        } else {
            retNodeSet.addAll(tc.getParentTC().getShardingNodes());
        }
        return retNodeSet;
    }

    public static Set<String> ruleCalculate(String schemaName, RouteResultset rrs, ShardingTableConfig tc, ColumnRoute columnRoute, boolean ignoreNull, String clientCharset) {
        Set<String> routeNodeSet = new LinkedHashSet<>();
        if (columnRoute.getColValue() != null) {
            Object originValue = columnRoute.getColValue();
            if (originValue instanceof String) {
                String value = (String) originValue;
                //for explain
                if (NEED_REPLACE.equals(value) || ALL_SUB_QUERY_RESULTS.equals(value) ||
                        MIN_SUB_QUERY_RESULTS.equals(value) || MAX_SUB_QUERY_RESULTS.equals(value) || UpdateSubQueryHandler.NEED_REPLACE.equals(value)) {
                    return routeNodeSet;
                }
                if (!ignoreNull || !value.equalsIgnoreCase("null")) {
                    String shardingNode = ruleCalculateSingleValue(schemaName, tc, value, clientCharset);
                    routeNodeSet.add(shardingNode);
                }
            } else if (originValue instanceof Boolean) {
                if ((Boolean) originValue) { // true
                    String shardingNode = ruleCalculateSingleValue(schemaName, tc, "1", clientCharset);
                    routeNodeSet.add(shardingNode);
                } else { // false
                    return routeNodeSet;
                }
            } else if (!ignoreNull || originValue instanceof Number) {
                String shardingNode = ruleCalculateSingleValue(schemaName, tc, originValue, clientCharset);
                routeNodeSet.add(shardingNode);
            }
        } else if (columnRoute.getInValues() != null) {
            for (Object originValue : columnRoute.getInValues()) {
                if (originValue instanceof Boolean) {
                    if ((Boolean) originValue) { // true
                        ruleCalculateSingleValue(schemaName, tc, "1", clientCharset);
                    } else { // false
                        routeNodeSet.clear();
                        return routeNodeSet;
                    }
                } else {
                    if (originValue instanceof String) {
                        String value = (String) originValue;
                        //for explain
                        if (NEED_REPLACE.equals(value) || ALL_SUB_QUERY_RESULTS.equals(value) ||
                                MIN_SUB_QUERY_RESULTS.equals(value) || MAX_SUB_QUERY_RESULTS.equals(value)) {
                            return routeNodeSet;
                        }
                    }
                    String shardingNode = ruleCalculateSingleValue(schemaName, tc, originValue, clientCharset);
                    routeNodeSet.add(shardingNode);
                }
            }
        }
        ruleCalculateRangeValue(rrs, tc, columnRoute, routeNodeSet);
        return routeNodeSet;
    }

    private static void ruleCalculateRangeValue(RouteResultset rrs, ShardingTableConfig tc, ColumnRoute columnRoute, Set<String> routeNodeSet) {
        if (columnRoute.getRangeValues() != null) {
            Set<String> rangeNodeSet = new LinkedHashSet<>();
            boolean isFirst = true;
            for (RangeValue rangeValue : columnRoute.getRangeValues()) { // get Intersection from all between and
                Integer[] nodeRange = tc.getFunction().calculateRange(String.valueOf(rangeValue.getBeginValue()), String.valueOf(rangeValue.getEndValue()));
                if (nodeRange != null) {
                    if (isFirst) {
                        if (nodeRange.length == 0) {
                            rangeNodeSet.addAll(tc.getShardingNodes());
                        } else {
                            String shardingNode;
                            for (Integer nodeId : nodeRange) {
                                shardingNode = tc.getShardingNodes().get(nodeId);
                                rangeNodeSet.add(shardingNode);
                            }
                        }
                        isFirst = false;
                    } else {
                        if (nodeRange.length == 0) {
                            rangeNodeSet.retainAll(tc.getShardingNodes());
                        } else {
                            String shardingNode;
                            Set<String> tmpNodeSet = new LinkedHashSet<>();
                            for (Integer nodeId : nodeRange) {
                                shardingNode = tc.getShardingNodes().get(nodeId);
                                tmpNodeSet.add(shardingNode);
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
                if (DTRACE_LOGGER.isTraceEnabled()) {
                    DTRACE_LOGGER.trace("all ColumnRoute " + columnRoute + " merge to always false");
                }
                rrs.setAlwaysFalse(true);
                rangeNodeSet.addAll(tc.getShardingNodes());
            }
            if (DTRACE_LOGGER.isTraceEnabled()) {
                DTRACE_LOGGER.trace("all ColumnRoute " + columnRoute + " merge to these node:" + routeNodeSet);
            }
        }
    }

    private static String ruleCalculateSingleValue(String schemaName, ShardingTableConfig tc, Object originValue, String clientCharset) {
        String value;
        if (originValue instanceof SQLHexExpr) {
            TableMeta orgTbMeta;
            try {
                orgTbMeta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaName,
                        tc.getName());
            } catch (SQLNonTransientException e) {
                throw new RuntimeException(e.getMessage());
            }
            String dataType = null;
            for (ColumnMeta column : orgTbMeta.getColumns()) {
                if (column.getName().equalsIgnoreCase(tc.getShardingColumn())) {
                    dataType = column.getDataType();
                    break;
                }
            }
            if (FieldUtil.isNumberType(dataType)) {
                value = Long.parseLong(((SQLHexExpr) originValue).getHex(), 16) + "";
            } else {
                value = HexFormatUtil.fromHex(((SQLHexExpr) originValue).getHex(), CharsetUtil.getJavaCharset(clientCharset));
            }
        } else {
            String actualCharset = CharsetContext.remove();
            value = StringUtil.charsetReplace(clientCharset, actualCharset, originValue.toString());
        }
        Integer nodeIndex = tc.getFunction().calculate(value);
        if (nodeIndex == null) {
            String msg = "can't find any valid shardingNode in table[" + tc.getName() +
                    "] -> column[" + tc.getShardingColumn() + "] -> value[" + value + "]";
            LOGGER.info(msg);
            throw new IllegalArgumentException(msg);
        }
        if (nodeIndex < 0 || nodeIndex >= tc.getShardingNodes().size()) {
            String msg = "Can't find a valid shardingNode for specified node index in table[" + tc.getName() +
                    "] -> column[" + tc.getShardingColumn() + "] -> value[" + value + "]" + ",Index : " + nodeIndex;
            LOGGER.info(msg);
            throw new IllegalArgumentException(msg);
        }
        return tc.getShardingNodes().get(nodeIndex);
    }

    /**
     * DBLE0REQ-504
     *
     * @param ctx
     * @param selectStmt
     * @return
     */
    public static boolean canMergeJoin(DruidShardingParseInfo ctx, SQLSelectStatement selectStmt) {
        SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
        if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
            //check the select into sql is not supported
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) sqlSelectQuery;

            //three types of select route according to the from item in select sql
            SQLTableSource mysqlFrom = mysqlSelectQuery.getFrom();
            boolean isShardingRelationship = true;
            for (TableStat.Relationship relationship : ctx.getRelationship()) {
                boolean leftFlag = isShardingColumn(relationship.getLeft());
                boolean rightFlag = isShardingColumn(relationship.getRight());
                isShardingRelationship = isShardingRelationship && leftFlag && rightFlag;
            }
            return isInnerJoin(mysqlFrom) && isShardingRelationship;
        }
        return false;
    }

    private static boolean isInnerJoin(SQLTableSource tableSource) {
        boolean canMerge;
        if (tableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource joinTableSource = (SQLJoinTableSource) tableSource;
            SQLTableSource left = joinTableSource.getLeft();
            SQLTableSource right = joinTableSource.getRight();

            SQLJoinTableSource.JoinType joinType = joinTableSource.getJoinType();
            //on condition
            canMerge = isInnerJoin(left) && isInnerJoin(right) && (joinType.equals(SQLJoinTableSource.JoinType.INNER_JOIN) || joinType.equals(SQLJoinTableSource.JoinType.JOIN) || joinType.equals(SQLJoinTableSource.JoinType.CROSS_JOIN) || joinType.equals(SQLJoinTableSource.JoinType.STRAIGHT_JOIN));
        } else {
            canMerge = true;
        }
        return canMerge;
    }

    private static boolean isShardingColumn(TableStat.Column column) {
        if (null == column) {
            return false;
        }
        String columnName = column.getName();
        boolean isSharding = false;
        String table = column.getTable();
        if (null == table) {
            return false;
        }
        String[] split = table.split("\\.");
        if (split.length == 2) {
            String schemaName = split[0];
            String tableName = split[1];
            SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
            BaseTableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig instanceof ShardingTableConfig) {
                ShardingTableConfig shardingTableConfig = (ShardingTableConfig) tableConfig;
                String partitionCol = shardingTableConfig.getShardingColumn();
                isSharding = StringUtil.equalsIgnoreCase(partitionCol, columnName);
            } else if (tableConfig instanceof ChildTableConfig) {
                ChildTableConfig childTableConfig = (ChildTableConfig) tableConfig;
                String joinColumn = childTableConfig.getJoinColumn();
                isSharding = StringUtil.equalsIgnoreCase(joinColumn, columnName);
            }
        }
        return isSharding;
    }

    public static String tryRouteTablesToOneNodeForComplex(
            RouteResultset rrs, DruidShardingParseInfo ctx,
            Set<String> schemaList, int tableSize, String clientCharset, SQLSelectStatement selectStmt) throws SQLException {
        Set<String> tmpResultNodes = new HashSet<>();
        Set<Pair<String, String>> tablesSet = new HashSet<>(ctx.getTables());
        Set<Pair<String, BaseTableConfig>> globalTables = new HashSet<>();
        int unrepeatedTableSize = ctx.getTables().size();
        extractSchema(ctx, schemaList);
        if (unrepeatedTableSize != tableSize && (!canMergeJoin(ctx, selectStmt))) {
            //DBLE0REQ-504
            return null;
        }
        for (Pair<String, String> table : ctx.getTables()) {
            String schemaName = table.getKey();
            String tableName = table.getValue();
            SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
            BaseTableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig == null) {
                if (tryRouteNoShardingTablesToOneNode(tmpResultNodes, tablesSet, table, schemaName, tableName, schema))
                    return null;
            } else if (tableConfig instanceof GlobalTableConfig) {
                globalTables.add(new Pair<>(schemaName, tableConfig));
            } else if (tableConfig instanceof SingleTableConfig) {
                tmpResultNodes.add(schema.getTables().get(tableName).getShardingNodes().get(0));
                tablesSet.remove(table);
                if (tmpResultNodes.size() != 1) {
                    return null;
                }
            }
        }
        if (globalTables.size() == tableSize) {
            return tryRouteGlobalTablesToOneNode(tmpResultNodes, globalTables);
        }

        return tryCalculateRouteTablesToOneNodeForComplex(rrs, ctx, tmpResultNodes, globalTables, tablesSet, clientCharset);
    }

    private static void extractSchema(DruidShardingParseInfo ctx, Set<String> schemaList) throws SQLException {
        for (Pair<String, String> table : ctx.getTables()) {
            String schemaName = table.getKey();
            String tableName = table.getValue();
            SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(schemaName);
            if (schema == null) {
                String msg = "Table " + StringUtil.getFullName(schemaName, tableName) + " doesn't exist";
                throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
            }
            schemaList.add(schemaName);
        }
    }

    private static String tryCalculateRouteTablesToOneNodeForComplex(
            RouteResultset rrs, DruidShardingParseInfo ctx,
            Set<String> tmpResultNodes,
            Set<Pair<String, BaseTableConfig>> globalTables, Set<Pair<String, String>> tablesSet,
            String clientCharset) throws SQLException {
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
                        BaseTableConfig tableConfig = schema.getTables().get(tableName);
                        if (!tryCalcNodeForShardingColumn(schemaName, rrs, tmpResultNodes, tablesSet, entry, table, tableConfig, clientCharset)) {
                            return null;
                        }
                    }
                }
                for (Pair<String, BaseTableConfig> table : globalTables) {
                    BaseTableConfig tb = table.getValue();
                    tmpResultNodes.retainAll(tb.getShardingNodes());
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

    private static String tryRouteGlobalTablesToOneNode(Set<String> tmpResultNodes, Set<Pair<String, BaseTableConfig>> globalTables) {
        boolean isFirstTable = true;
        for (Pair<String, BaseTableConfig> table : globalTables) {
            BaseTableConfig tb = table.getValue();
            if (isFirstTable) {
                tmpResultNodes.addAll(tb.getShardingNodes());
                isFirstTable = false;
            } else {
                tmpResultNodes.retainAll(tb.getShardingNodes());
            }
        }
        if (tmpResultNodes.size() != 0) {
            return getRandomShardingNode(new ArrayList<>(tmpResultNodes));
        } else {
            return null;
        }
    }

    private static boolean tryRouteNoShardingTablesToOneNode(Set<String> tmpResultNodes, Set<Pair<String, String>> tablesSet, Pair<String, String> table, String schemaName, String tableName, SchemaConfig schema) throws SQLNonTransientException {
        //may view
        if (ProxyMeta.getInstance().getTmManager().getSyncView(schemaName, tableName) != null) {
            return true;
        }
        if (schema.getDefaultShardingNodes() == null) {
            String msg = " Table '" + schemaName + "." + tableName + "' doesn't exist";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        } else {
            tmpResultNodes.addAll(schema.getDefaultShardingNodes());
            tablesSet.remove(table);
            if (tmpResultNodes.size() != 1) {
                return true;
            }
        }
        return false;
    }


    public static boolean tryCalcNodeForShardingColumn(
            String schemaName, RouteResultset rrs, Set<String> resultNodes, Set<Pair<String, String>> tablesSet,
            Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry, Pair<String, String> table,
            BaseTableConfig tableConfig, String clientCharset) throws SQLNonTransientException {
        if (tableConfig == null) {
            return false; //  alias table, may subquery
        }


        // where filter contains partition column

        Map<String, ColumnRoute> columnsMap = entry.getValue();
        if (tableConfig instanceof ShardingTableConfig) {
            ShardingTableConfig shardingTableConfig = (ShardingTableConfig) tableConfig;
            String partitionCol = shardingTableConfig.getShardingColumn();
            ColumnRoute partitionValue = columnsMap.get(partitionCol);
            if (partitionValue == null) {
                return false;
            }
            try {
                Set<String> shardingNodeSet = ruleCalculate(schemaName, rrs, shardingTableConfig, partitionValue, false, clientCharset);
                resultNodes.addAll(shardingNodeSet);
            } catch (Exception e) { //complex filter
                return true;
            }
            tablesSet.remove(table);
            if (resultNodes.size() != 1) {
                return false;
            }
        } else if (tableConfig instanceof ChildTableConfig) {
            ChildTableConfig childTableConfig = (ChildTableConfig) tableConfig;
            String joinColumn = childTableConfig.getJoinColumn();
            ColumnRoute joinColumnValue = columnsMap.get(joinColumn);
            if (joinColumnValue == null) {
                return false;
            }
            Set<String> shardingNodeSet = ruleByJoinValueCalculate(schemaName, rrs, (ChildTableConfig) tableConfig, joinColumnValue, clientCharset);
            if (shardingNodeSet.size() > 1) {
                return false;
            }
            resultNodes.addAll(shardingNodeSet);
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
    public static RouteResultset tryRouteForTables(
            SchemaConfig schema, DruidShardingParseInfo ctx, RouteCalculateUnit routeUnit, RouteResultset rrs,
            boolean isSelect, String clientCharset) throws SQLException {
        List<Pair<String, String>> tables = ctx.getTables();
        Pair<String, String> firstTable = tables.get(0);
        // no sharding table
        String noShardingNode = RouterUtil.isNoSharding(schema, firstTable.getValue());
        if (noShardingNode != null) {
            return RouterUtil.routeToSingleNode(rrs, noShardingNode, Sets.newHashSet(schema.getName() + "." + firstTable.getValue()));
        }

        if (tables.size() == 1) {
            return RouterUtil.tryRouteForOneTable(schema, routeUnit, firstTable.getValue(), rrs, isSelect, clientCharset);
        }

        /*
         * multi-table it must be ER OR   global* normal , global* er
         */
        //map <table,sharding_nodes>
        Map<Pair<String, String>, Set<String>> tablesRouteMap = new HashMap<>();

        Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if (tablesAndConditions != null && tablesAndConditions.size() > 0) {
            //findRouter for shard-ing table
            RouterUtil.findRouterForTablesInOneSchema(schema, rrs, tablesAndConditions, tablesRouteMap, false, clientCharset);
            if (rrs.isFinishedRoute()) {
                return rrs;
            }
        }

        //findRouter for singe table global table will not change the result
        // if global table and normal table has no intersection ,they had treat as normal join
        Set<String> tableSet = Sets.newHashSet();
        for (Pair<String, String> table : tables) {
            String tableName = table.getValue();
            tableSet.add(schema.getName() + "." + tableName);
            String testShardingNode = RouterUtil.isNoSharding(schema, tableName);
            if (testShardingNode != null && tablesRouteMap.size() == 0) {
                return RouterUtil.routeToSingleNode(rrs, testShardingNode, Sets.newHashSet(schema.getName() + "." + tableName));
            }
            BaseTableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig != null && !(tableConfig instanceof GlobalTableConfig) && tablesRouteMap.get(table) == null) { //the other is single table
                tablesRouteMap.put(table, new HashSet<>());
                tablesRouteMap.get(table).addAll(tableConfig.getShardingNodes());
            }
        }

        Set<String> retNodesSet = retainRouteMap(tablesRouteMap);
        if (retNodesSet.size() == 0 && DTRACE_LOGGER.isTraceEnabled()) {
            DTRACE_LOGGER.trace("this RouteCalculateUnit is always false, so ignore:" + routeUnit);
        }
        routeToMultiNode(isSelect, rrs, retNodesSet, tableSet);
        return rrs;

    }


    /**
     * tryRouteFor multiTables
     */
    private static RouteResultset tryRouteForTablesComplex(
            Map<Pair<String, String>, SchemaConfig> schemaMap, DruidShardingParseInfo ctx,
            RouteCalculateUnit routeUnit, RouteResultset rrs, String clientCharset)
            throws SQLException {

        List<Pair<String, String>> tables = ctx.getTables();

        Pair<String, String> firstTable = tables.get(0);
        SchemaConfig schemaConfig = schemaMap.get(firstTable);
        // no sharding table
        String noShardingNode = RouterUtil.isNoSharding(schemaConfig, firstTable.getValue());
        if (noShardingNode != null) {
            return RouterUtil.routeToSingleNode(rrs, noShardingNode, Sets.newHashSet(schemaConfig.getName() + "." + firstTable.getValue()));
        }

        if (tables.size() == 1) {
            return RouterUtil.tryRouteForOneTable(schemaMap.get(firstTable), routeUnit, firstTable.getValue(), rrs, true, clientCharset);
        }

        /*
         * multi-table it must be ER OR   global* normal , global* er
         */
        //map <table,sharding_nodes>
        Map<Pair<String, String>, Set<String>> tablesRouteMap = new HashMap<>();

        Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if (tablesAndConditions != null && tablesAndConditions.size() > 0) {
            //findRouter for shard-ing table
            RouterUtil.findRouterForMultiSchemaTables(schemaMap, rrs, tablesAndConditions, tablesRouteMap, clientCharset);
            if (rrs.isFinishedRoute()) {
                return rrs;
            }
        }

        //findRouter for singe table * global table will not change the result
        // if global table and normal table has no intersection ,they had treat as normal join
        Set<String> tableSet = Sets.newHashSet();
        for (Pair<String, String> table : tables) {
            SchemaConfig schema = DbleServer.getInstance().getConfig().getSchemas().get(table.getKey());
            String tableName = table.getValue();
            tableSet.add(schema.getName() + "." + tableName);
            String testShardingNode = RouterUtil.isNoSharding(schema, tableName);
            if (testShardingNode != null && tablesRouteMap.size() == 0) {
                return RouterUtil.routeToSingleNode(rrs, testShardingNode, Sets.newHashSet(schema.getName() + "." + tableName));
            }
            BaseTableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig != null && !(tableConfig instanceof GlobalTableConfig) && tablesRouteMap.get(table) == null) { //the other is single table
                tablesRouteMap.put(table, new HashSet<>());
                tablesRouteMap.get(table).addAll(tableConfig.getShardingNodes());
            }
        }

        Set<String> retNodesSet = retainRouteMap(tablesRouteMap);
        if (retNodesSet.size() == 0 && DTRACE_LOGGER.isTraceEnabled()) {
            DTRACE_LOGGER.trace("this RouteCalculateUnit is always false, so ignore:" + routeUnit);
        }
        routeToMultiNode(true, rrs, retNodesSet, tableSet);
        return rrs;

    }

    public static Set<String> retainRouteMap(Map<Pair<String, String>, Set<String>> tablesRouteMap) throws SQLNonTransientException {
        Set<String> retNodesSet = new HashSet<>();
        boolean isFirstAdd = true;
        for (Map.Entry<Pair<String, String>, Set<String>> entry : tablesRouteMap.entrySet()) {
            if (entry.getValue() == null || entry.getValue().size() == 0) {
                throw new SQLNonTransientException("parent key can't find any valid shardingNode ");
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
    public static RouteResultset tryRouteForOneTable(
            SchemaConfig schema, RouteCalculateUnit routeUnit, String tableName, RouteResultset rrs,
            boolean isSelect, String clientCharset) throws SQLException {
        BaseTableConfig tc = schema.getTables().get(tableName);
        if (tc == null) {
            String msg = "Table '" + schema.getName() + "." + tableName + "' doesn't exist";
            throw new SQLException(msg, "42S02", ErrorCode.ER_NO_SUCH_TABLE);
        }

        if (tc instanceof GlobalTableConfig) {
            if (isSelect) {
                // global select ,not cache route result
                rrs.setSqlRouteCacheAble(false);
                rrs.setGlobalTable(true);
                String randomShardingNode = RouterUtil.getRandomShardingNode(tc.getShardingNodes());
                rrs = routeToSingleNode(rrs, randomShardingNode, Sets.newHashSet(schema.getName() + "." + tableName));
                List<String> globalBackupNodes = new ArrayList<>(tc.getShardingNodes().size() - 1);
                for (String shardingNode : tc.getShardingNodes()) {
                    if (!shardingNode.equals(randomShardingNode)) {
                        globalBackupNodes.add(shardingNode);
                    }
                }
                rrs.setGlobalBackupNodes(globalBackupNodes);
                return rrs;
            } else { //insert into all global table's node
                return routeToMultiNode(false, rrs, tc.getShardingNodes(), true, Sets.newHashSet(schema.getName() + "." + tableName));
            }
        } else if (tc instanceof SingleTableConfig) {
            return routeToSingleNode(rrs, tc.getShardingNodes().get(0), Sets.newHashSet(schema.getName() + "." + tableName));
        } else if (tc instanceof ChildTableConfig) {
            ChildTableConfig childConfig = (ChildTableConfig) tc;
            if ((childConfig.getParentTC() != null && childConfig.getDirectRouteTC() == null)) {
                // one of the children of complex ER table
                return routeToMultiNode(rrs.isSqlRouteCacheAble(), rrs, tc.getShardingNodes(), Sets.newHashSet(schema.getName() + "." + tableName));
            } else {
                Pair<String, String> table = new Pair<>(schema.getName(), tableName);
                Map<Pair<String, String>, Set<String>> tablesRouteMap = new HashMap<>();
                if (routeUnit.getTablesAndConditions() != null && routeUnit.getTablesAndConditions().size() > 0) {
                    RouterUtil.findRouterForTablesInOneSchema(schema, rrs, routeUnit.getTablesAndConditions(), tablesRouteMap, true, clientCharset);
                    if (rrs.isFinishedRoute()) {
                        return rrs;
                    }
                }
                if (tablesRouteMap.get(table) == null) {
                    return routeToMultiNode(rrs.isSqlRouteCacheAble(), rrs, tc.getShardingNodes(), Sets.newHashSet(schema.getName() + "." + tableName));
                } else {
                    return routeToMultiNode(rrs.isSqlRouteCacheAble(), rrs, tablesRouteMap.get(table), Sets.newHashSet(schema.getName() + "." + tableName));
                }
            }
        } else { //shard-ing table
            Pair<String, String> table = new Pair<>(schema.getName(), tableName);
            if (!checkSQLRequiredSharding(schema, routeUnit, (ShardingTableConfig) tc, table)) {
                throw new IllegalArgumentException("sqlRequiredSharding is true,the table '" + schema.getName() + "." +
                        tc.getName() + "' requires the condition in sql to include the sharding column '" + ((ShardingTableConfig) tc).getShardingColumn() + "'");

            }
            Map<Pair<String, String>, Set<String>> tablesRouteMap = new HashMap<>();
            if (routeUnit.getTablesAndConditions() != null && routeUnit.getTablesAndConditions().size() > 0) {
                RouterUtil.findRouterForTablesInOneSchema(schema, rrs, routeUnit.getTablesAndConditions(), tablesRouteMap, true, clientCharset);
                if (rrs.isFinishedRoute()) {
                    return rrs;
                }
            }
            if (tablesRouteMap.get(table) == null) {
                return routeToMultiNode(rrs.isSqlRouteCacheAble(), rrs, tc.getShardingNodes(), Sets.newHashSet(schema.getName() + "." + tableName));
            } else {
                return routeToMultiNode(rrs.isSqlRouteCacheAble(), rrs, tablesRouteMap.get(table), Sets.newHashSet(schema.getName() + "." + tableName));
            }
        }
    }

    /**
     * @param schema SchemaConfig
     * @param tc     TableConfig
     * @return true for passed
     */
    private static boolean checkSQLRequiredSharding(SchemaConfig schema, RouteCalculateUnit routeUnit, ShardingTableConfig tc, Pair<String, String> table) {
        if (!tc.isSqlRequiredSharding()) {
            return true;
        }
        boolean hasRequiredValue = false;
        if (routeUnit.getTablesAndConditions().get(table) != null && routeUnit.getTablesAndConditions().get(table).size() != 0) {
            for (Map.Entry<String, ColumnRoute> condition : routeUnit.getTablesAndConditions().get(table).entrySet()) {
                String colName = RouterUtil.getFixedSql(RouterUtil.removeSchema(condition.getKey(), schema.getName()));
                //condition is partition column
                if (colName.equals(tc.getShardingColumn())) {
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
            Map<Pair<String, String>, Set<String>> tablesRouteMap, String clientCharset) throws SQLNonTransientException {

        //router for shard-ing tables
        for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : tablesAndConditions.entrySet()) {
            Pair<String, String> table = entry.getKey();
            String tableName = table.getValue();
            SchemaConfig schema = schemaMap.get(table);
            BaseTableConfig tableConfig = schema.getTables().get(tableName);

            if (tableConfig != null) {
                if ((tableConfig instanceof ShardingTableConfig)) {
                    if (findRouterWithConditionsForShardingTable(schema.getName(), rrs, tablesRouteMap, table, (ShardingTableConfig) tableConfig, entry.getValue(), clientCharset))
                        return;
                } else if (tableConfig instanceof ChildTableConfig) {
                    if (findRouterWithConditionsForChildTable(schema.getName(), rrs, tablesRouteMap, table, (ChildTableConfig) tableConfig, entry.getValue(), clientCharset))
                        return;
                }
            }
        }
    }


    /**
     * findRouterForMultiSchemaTables
     */
    private static void findRouterForTablesInOneSchema(
            SchemaConfig schema, RouteResultset rrs, Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions,
            Map<Pair<String, String>, Set<String>> tablesRouteMap,
            boolean isSingleTable, String clientCharset) throws SQLNonTransientException {

        //router for shard-ing tables
        for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : tablesAndConditions.entrySet()) {
            Pair<String, String> table = entry.getKey();
            String tableName = table.getValue();
            BaseTableConfig tableConfig = schema.getTables().get(tableName);
            if (tableConfig == null) {
                if (isSingleTable) {
                    String msg = " Table '" + schema.getName() + "." + tableName + "' doesn't exist";
                    LOGGER.info(msg);
                    throw new SQLNonTransientException(msg);
                } else {
                    //cross to other sharding
                    continue;
                }
            }
            //shard-ing table,childTable or others . global table or single node shard-ing table will router later
            if ((tableConfig instanceof ShardingTableConfig)) {
                if (findRouterWithConditionsForShardingTable(schema.getName(), rrs, tablesRouteMap, table, (ShardingTableConfig) tableConfig, entry.getValue(), clientCharset))
                    return;
            } else if (tableConfig instanceof ChildTableConfig) {
                if (findRouterWithConditionsForChildTable(schema.getName(), rrs, tablesRouteMap, table, (ChildTableConfig) tableConfig, entry.getValue(), clientCharset))
                    return;
            }
        }
    }

    private static boolean findRouterWithConditionsForShardingTable(
            String schemaName, RouteResultset rrs, Map<Pair<String, String>, Set<String>> tablesRouteMap,
            Pair<String, String> table, ShardingTableConfig tableConfig,
            Map<String, ColumnRoute> columnsMap, String clientCharset) throws SQLNonTransientException {

        String partitionCol = tableConfig.getShardingColumn();
        boolean isFoundPartitionValue = partitionCol != null && columnsMap.get(partitionCol) != null;

        // where filter contains partition column
        if (isFoundPartitionValue) {
            ColumnRoute partitionValue = columnsMap.get(partitionCol);
            Set<String> shardingNodeSet = ruleCalculate(schemaName, rrs, tableConfig, partitionValue, rrs.isComplexSQL(), clientCharset);
            if (shardingNodeSet.size() > 0) {
                tablesRouteMap.computeIfAbsent(table, k -> new HashSet<>());
                tablesRouteMap.get(table).addAll(shardingNodeSet);
            }
        } else {
            //no partition column,router to all nodes
            tablesRouteMap.computeIfAbsent(table, k -> new HashSet<>());
            tablesRouteMap.get(table).addAll(tableConfig.getShardingNodes());
        }
        return false;
    }

    private static boolean findRouterWithConditionsForChildTable(
            String schemaName, RouteResultset rrs, Map<Pair<String, String>, Set<String>> tablesRouteMap,
            Pair<String, String> table, ChildTableConfig tableConfig,
            Map<String, ColumnRoute> columnsMap, String clientCharset) throws SQLNonTransientException {

        String joinColumn = tableConfig.getJoinColumn();

        // where filter contains partition column
        if (joinColumn != null && columnsMap.get(joinColumn) != null) {
            routerForJoinTable(schemaName, rrs, tableConfig, columnsMap, joinColumn, clientCharset);
            return true;
        } else {
            //no partition column,router to all nodes
            tablesRouteMap.computeIfAbsent(table, k -> new HashSet<>());
            tablesRouteMap.get(table).addAll(tableConfig.getShardingNodes());
        }
        return false;
    }


    private static void routerForJoinTable(
            String schemaName, RouteResultset rrs, ChildTableConfig tableConfig, Map<String, ColumnRoute> columnsMap,
            String joinColumn, String clientCharset) throws SQLNonTransientException {
        //childTable  (if it's ER JOIN of select)must find root table,remove childTable, only left root table
        ColumnRoute joinColumnValue = columnsMap.get(joinColumn);
        Set<String> shardingNodeSet = ruleByJoinValueCalculate(schemaName, rrs, tableConfig, joinColumnValue, clientCharset);

        if (shardingNodeSet.isEmpty()) {
            throw new SQLNonTransientException(
                    "parent key can't find any valid shardingNode ");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("found partition nodes (using parent partition rule directly) for child table to update  " +
                    Arrays.toString(shardingNodeSet.toArray()) + " sql :" + rrs.getStatement());
        }
        if (shardingNodeSet.size() > 1) {
            routeToMultiNode(rrs.isSqlRouteCacheAble(), rrs, shardingNodeSet, Sets.newHashSet(schemaName + "." + tableConfig.getName()));
        } else {
            rrs.setSqlRouteCacheAble(true);
            routeToSingleNode(rrs, shardingNodeSet.iterator().next(), Sets.newHashSet(schemaName + "." + tableConfig.getName()));
        }
    }


    public static String isNoSharding(SchemaConfig schemaConfig, String tableName) throws SQLNonTransientException {
        if (schemaConfig == null || ProxyMeta.getInstance().getTmManager().getSyncView(schemaConfig.getName(), tableName) instanceof QueryNode) {
            return null;
        }
        if (schemaConfig.isNoSharding()) { //sharding without table
            return schemaConfig.getDefaultSingleNode();
        }
        BaseTableConfig tbConfig = schemaConfig.getTables().get(tableName);
        if (tbConfig == null && schemaConfig.isDefaultSingleNode()) {
            return schemaConfig.getDefaultSingleNode();
        }
        if (tbConfig != null && tbConfig instanceof SingleTableConfig) {
            return tbConfig.getShardingNodes().get(0);
        }
        return null;
    }

    public static String isNoShardingDDL(SchemaConfig schemaConfig, String tableName) {
        if (schemaConfig == null) {
            return null;
        }
        if (schemaConfig.isNoSharding()) { //sharding without table
            return schemaConfig.getDefaultSingleNode();
        }
        BaseTableConfig tbConfig = schemaConfig.getTables().get(tableName);
        if (tbConfig == null && schemaConfig.isDefaultSingleNode()) {
            return schemaConfig.getDefaultSingleNode();
        }
        if (tbConfig != null && (tbConfig instanceof SingleTableConfig)) {
            return tbConfig.getShardingNodes().get(0);
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

    public static boolean isAllGlobalTable(DruidShardingParseInfo ctx, SchemaConfig schema) {
        boolean isAllGlobal = false;
        for (Pair<String, String> table : ctx.getTables()) {
            BaseTableConfig tableConfig = schema.getTables().get(table.getValue());
            if (tableConfig != null && tableConfig instanceof GlobalTableConfig) {
                isAllGlobal = true;
            } else {
                return false;
            }
        }
        return isAllGlobal;
    }

    /**
     * clickhouse and mysql syntax incompatibility
     *
     * @param selectQuery
     * @return
     */
    public static boolean checkSQLNotSupport(SQLSelectQuery selectQuery) {
        if (selectQuery instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) selectQuery;
            //only support distinct
            if (mysqlSelectQuery.getDistionOption() != 0 && mysqlSelectQuery.getDistionOption() != SQLSetQuantifier.DISTINCT) {
                return true;
            }
            //    [HIGH_PRIORITY]
            if (mysqlSelectQuery.isHignPriority()) {
                return true;
            }
            //    [STRAIGHT_JOIN]
            if (mysqlSelectQuery.isStraightJoin()) {
                return true;
            }
            //    [SQL_SMALL_RESULT] [SQL_BIG_RESULT] [SQL_BUFFER_RESULT]
            if (mysqlSelectQuery.isSmallResult() || mysqlSelectQuery.isBigResult() || mysqlSelectQuery.isBufferResult()) {
                return true;
            }
            //    [SQL_NO_CACHE|SQL_CACHE] [SQL_CALC_FOUND_ROWS]
            if (mysqlSelectQuery.getCache() != null || mysqlSelectQuery.isCalcFoundRows()) {
                return true;
            }
            //[WINDOW window_name AS (window_spec)
            //        [, window_name AS (window_spec)] ...]
            if (mysqlSelectQuery.getWindows() != null && !mysqlSelectQuery.getWindows().isEmpty()) {
                return true;
            }
            //[FOR {UPDATE | SHARE}
            //        [OF tbl_name [, tbl_name] ...]
            //        [NOWAIT | SKIP LOCKED]
            //      | LOCK IN SHARE MODE]
            if (mysqlSelectQuery.isForUpdate() || mysqlSelectQuery.isForShare() || mysqlSelectQuery.isNoWait() || mysqlSelectQuery.isSkipLocked() || mysqlSelectQuery.isLockInShareMode()) {
                return true;
            }
            //select-variables
            boolean containVariables = mysqlSelectQuery.getSelectList().stream().anyMatch(selectItem -> selectItem.getExpr() instanceof SQLVariantRefExpr);
            if (containVariables) {
                return true;
            }
            //where-variables
            SQLExpr whereExpr = mysqlSelectQuery.getWhere();
            containVariables = checkSQLNotSupportOfWhere(whereExpr);
            if (containVariables) {
                return true;
            }
            //from
            SQLTableSource tableSource = mysqlSelectQuery.getFrom();
            return checkSQLNotSupportOfTableSource(tableSource);
        } else if (selectQuery instanceof SQLUnionQuery) {
            SQLUnionQuery query = (SQLUnionQuery) selectQuery;
            List<SQLSelectQuery> relations = query.getRelations();
            for (SQLSelectQuery relation : relations) {
                if (checkSQLNotSupport(relation)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean checkSQLNotSupportOfWhere(SQLExpr whereExpr) {
        if (whereExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr tmp = (SQLBinaryOpExpr) whereExpr;
            if (tmp.getLeft() instanceof SQLBinaryOpExpr) {
                return checkSQLNotSupportOfWhere(tmp.getLeft()) || checkSQLNotSupportOfWhere(tmp.getRight());
            } else {
                return tmp.getLeft() instanceof SQLVariantRefExpr || tmp.getRight() instanceof SQLVariantRefExpr;
            }
        }
        return false;
    }


    private static boolean checkSQLNotSupportOfTableSource(SQLTableSource tableSource) {
        if (tableSource instanceof SQLExprTableSource) {
            SQLExprTableSource exprTableSource = (SQLExprTableSource) tableSource;
            if (exprTableSource.getPartitionSize() != 0) {
                return true;
            }
        } else if (tableSource instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource fromSource = (SQLSubqueryTableSource) tableSource;
            SQLSelectQuery sqlSelectQuery = fromSource.getSelect().getQuery();
            return checkSQLNotSupport(sqlSelectQuery);
        } else if (tableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource fromSource = (SQLJoinTableSource) tableSource;
            SQLTableSource left = fromSource.getLeft();
            if (checkSQLNotSupportOfTableSource(left)) {
                return true;
            }
            SQLTableSource right = fromSource.getRight();
            return checkSQLNotSupportOfTableSource(right);
        } else if (tableSource instanceof SQLUnionQueryTableSource) {
            SQLUnionQueryTableSource fromSource = (SQLUnionQueryTableSource) tableSource;
            SQLUnionQuery unionQuery = fromSource.getUnion();
            return checkSQLNotSupport(unionQuery);
        }
        return false;
    }


    /**
     * check contains aggregate function
     *
     * @param selectQuery
     * @return
     */
    public static boolean checkFunction(SQLSelectQuery selectQuery) {
        boolean isAggregate;
        if (selectQuery instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) selectQuery;
            if (mysqlSelectQuery.getGroupBy() != null) {
                return true;
            }
            //select item
            List<String> aggregateFunctionList = Lists.newArrayList("AVG", "COUNT", "MAX", "MIN", "SUM", "STDDEV_POP", "STDDEV_SAMP", "VAR_POP", "VAR_SAMP");
            for (SQLSelectItem sqlSelectItem : mysqlSelectQuery.getSelectList()) {
                SQLExpr expr = sqlSelectItem.getExpr();
                if (expr instanceof SQLMethodInvokeExpr) {
                    SQLMethodInvokeExpr aggregateExpr = (SQLMethodInvokeExpr) expr;
                    String methodName = aggregateExpr.getMethodName();
                    isAggregate = aggregateFunctionList.contains(methodName.toLowerCase()) || aggregateFunctionList.contains(methodName.toUpperCase());
                    if (isAggregate) {
                        return true;
                    }
                }
            }
            //from
            SQLTableSource tableSource = mysqlSelectQuery.getFrom();
            return checkFunctionOfTableSource(tableSource);
        } else if (selectQuery instanceof SQLUnionQuery) {
            SQLUnionQuery query = (SQLUnionQuery) selectQuery;
            List<SQLSelectQuery> relations = query.getRelations();
            for (SQLSelectQuery relation : relations) {
                if (checkFunction(relation)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean checkFunctionOfTableSource(SQLTableSource tableSource) {
        if (tableSource == null || tableSource instanceof SQLExprTableSource) {
            return false;
        } else if (tableSource instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource fromSource = (SQLSubqueryTableSource) tableSource;
            SQLSelectQuery sqlSelectQuery = fromSource.getSelect().getQuery();
            return checkFunction(sqlSelectQuery);
        } else if (tableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource fromSource = (SQLJoinTableSource) tableSource;
            SQLTableSource left = fromSource.getLeft();
            if (checkFunctionOfTableSource(left)) {
                return true;
            }
            SQLTableSource right = fromSource.getRight();
            return checkFunctionOfTableSource(right);
        } else if (tableSource instanceof SQLUnionQueryTableSource) {
            SQLUnionQueryTableSource fromSource = (SQLUnionQueryTableSource) tableSource;
            SQLUnionQuery unionQuery = fromSource.getUnion();
            return checkFunction(unionQuery);
        }
        return false;
    }
}
