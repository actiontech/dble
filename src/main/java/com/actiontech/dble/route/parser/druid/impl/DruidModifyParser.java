package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.*;
import com.actiontech.dble.config.privileges.ShardingPrivileges;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.RouteCalculateUnit;
import com.actiontech.dble.route.parser.druid.RouteTableConfigInfo;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.route.util.ConditionUtil;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.sqlengine.mpp.ColumnRoute;
import com.actiontech.dble.util.CollectionUtil;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.stat.TableStat;
import com.google.common.collect.ImmutableList;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * Created by szf on 2020/6/2.
 */
abstract class DruidModifyParser extends DefaultDruidParser {

    static final String MODIFY_SQL_NOT_SUPPORT_MESSAGE = "This `INSERT ... SELECT Syntax` is not supported!";


    abstract SQLSelect acceptVisitor(SQLObject stmt, ServerSchemaStatVisitor visitor);

    /**
     * get sharding-column insert/replace index from sql object
     *
     */
    abstract int tryGetShardingColIndex(SchemaUtil.SchemaInfo schemaInfo, SQLStatement stmt, String partitionColumn) throws SQLNonTransientException;


    private void routeForSourceTable(ShardingTableConfig tc, RouteResultset rrs, Set<String> allNodeSet, SchemaConfig schema,
                                     RouteCalculateUnit routeUnit, ArrayList<String> partNodeList) throws SQLException {
        RouteResultset rrsTmp = RouterUtil.tryRouteForOneTable(schema, routeUnit, tc.getName(), rrs, true);
        if (rrsTmp != null && rrsTmp.getNodes() != null) {
            for (RouteResultsetNode n : rrsTmp.getNodes()) {
                partNodeList.add(n.getName());
                allNodeSet.add(n.getName());
            }
        }
    }

    /**
     * Category discussion the each table
     * + sharding table -- with same rule & has the sharding-column join relation
     * + child table -- has ER relation connection to a exists sharding table
     * + global table -- dataNodes cover
     * + single node table -- dataNodes cover
     *
     */
    private void checkForEachTableInSQL(Map<String, List<String>> notShardingTableMap, ArrayList<String> partNodeList, Pair<String, String> tn,
                                        RouteTableConfigInfo dataSourceTc, Set<TableStat.Relationship> relationships, Map<String, String> tableAliasMap) throws SQLException {
        String sName = tn.getKey();
        String tName = tn.getValue();
        SchemaConfig tSchema = DbleServer.getInstance().getConfig().getSchemas().get(sName);
        BaseTableConfig tConfig = tSchema.getTables().get(tName);

        if (tConfig != null) {
            if (tConfig instanceof ShardingTableConfig) {
                BaseTableConfig checkConfig = dataSourceTc.getTableConfig();
                if (checkConfig instanceof ShardingTableConfig && isSameSharding((ShardingTableConfig) tConfig, (ShardingTableConfig) checkConfig)) {
                    checkForShardingRelationship(relationships, tableAliasMap, dataSourceTc.getAlias() == null ? checkConfig.getName() : dataSourceTc.getAlias(), ((ShardingTableConfig) checkConfig).getShardingColumn(), (ShardingTableConfig) tConfig);
                } else {
                    throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                }
            } else if (tConfig instanceof GlobalTableConfig) {
                checkForGlobalRelationship(notShardingTableMap, (GlobalTableConfig) tConfig, partNodeList);
            } else if (tConfig instanceof SingleTableConfig) {
                checkForOneNodeRelationship(notShardingTableMap, tName, tConfig.getShardingNodes().get(0), partNodeList);
            } else if (tConfig instanceof ChildTableConfig) {
                checkForERRelationship(tableAliasMap, (ChildTableConfig) tConfig, relationships);
            }
        } else {
            checkForOneNodeRelationship(notShardingTableMap, tName, tSchema.getShardingNode(), partNodeList);
        }

    }


    private void checkForGlobalRelationship(Map<String, List<String>> notShardingTableMap, GlobalTableConfig tConfig, ArrayList<String> partNodeList) throws SQLException {
        notShardingTableMap.put(tConfig.getName(), tConfig.getShardingNodes());
        if (!tConfig.getShardingNodes().containsAll(partNodeList)) {
            throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
        }
    }

    private void checkForOneNodeRelationship(Map<String, List<String>> notShardingTableMap, String tableName, String dataNode, ArrayList<String> partNodeList) throws SQLException {
        ArrayList<String> list = new ArrayList<>();
        list.add(dataNode);
        notShardingTableMap.put(tableName, list);
        if (partNodeList.size() > 1 || !partNodeList.contains(dataNode)) {
            throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
        }
    }

    private void checkForShardingRelationship(Set<TableStat.Relationship> relationships, Map<String, String> tableAliasMap, String sourceTableName, String sourceColumnName,
                                              ShardingTableConfig tConfig) throws SQLException {
        TableStat.Column sourceColumn = new TableStat.Column(sourceTableName, sourceColumnName);
        ArrayList<String> rsAlias = findAliasByMap(tableAliasMap, tConfig.getName());
        List<TableStat.Column> rsColumnList = new ArrayList<>();
        for (String rsAlia : rsAlias) {
            rsColumnList.add(new TableStat.Column(rsAlia, tConfig.getShardingColumn()));
        }

        if (!findRelationship(relationships, sourceColumn, rsColumnList)) {
            throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
        }
    }

    private boolean findRelationship(Set<TableStat.Relationship> relationships, TableStat.Column sourceColumn, List<TableStat.Column> rsColumnList) {
        boolean hasReleation = false;
        for (TableStat.Relationship rs : relationships) {
            if (rs.getLeft().equals(sourceColumn) && rs.getOperator().equals("=")) {
                for (TableStat.Column rsColumn : rsColumnList) {
                    if (rs.getRight().equals(rsColumn)) {
                        hasReleation = true;
                        break;
                    }
                }
            } else if (rs.getRight().equals(sourceColumn) && rs.getOperator().equals("=")) {
                for (TableStat.Column rsColumn : rsColumnList) {
                    if (rs.getLeft().equals(rsColumn)) {
                        hasReleation = true;
                        break;
                    }
                }
            }
            if (hasReleation) {
                break;
            }
        }
        return hasReleation;
    }

    private void checkForERRelationship(Map<String, String> tableAliasMap, ChildTableConfig tConfig, Set<TableStat.Relationship> relationships) throws SQLException {
        ArrayList<String> selfAlias = findAliasByMap(tableAliasMap, tConfig.getName());
        ArrayList<String> rsAlias = findAliasByMap(tableAliasMap, tConfig.getParentTC().getName());
        List<TableStat.Column> rsColumnList = new ArrayList<>();
        List<TableStat.Column> selfColumnList = new ArrayList<>();
        for (String rsAlia : rsAlias) {
            rsColumnList.add(new TableStat.Column(rsAlia, tConfig.getParentColumn()));
        }
        for (String sfAlia : selfAlias) {
            selfColumnList.add(new TableStat.Column(sfAlia, tConfig.getJoinColumn()));
        }
        if (rsAlias.size() > 1) {
            boolean hasFindRelation = false;
            for (TableStat.Column sourceColumn : selfColumnList) {
                if (findRelationship(relationships, sourceColumn, rsColumnList)) {
                    hasFindRelation = true;
                    break;
                }
            }
            if (!hasFindRelation) {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            }
        } else {
            throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
        }
    }

    /**
     * check the condition in signle Route Unit
     * check the each table has the relation to connection to data-from table
     *
     */
    private void checkForShardingSingleRouteUnit(Map<String, List<String>> notShardingTableMap, RouteCalculateUnit routeUnit, RouteTableConfigInfo dataSourceTc,
                                                 ServerSchemaStatVisitor visitor, Map<String, String> tableAliasMap, Set<String> allNodeSet, SchemaConfig schema,
                                                 ShardingTableConfig tc, RouteResultset rrs) throws SQLException {
        Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if (tablesAndConditions != null) {
            Pair<String, String> key = new Pair<>(dataSourceTc.getSchema(), dataSourceTc.getTableConfig().getName());
            if (!CollectionUtil.containDuplicate(visitor.getSelectTableList(), dataSourceTc.getTableConfig().getName())) {
                ArrayList<String> partNodeList = new ArrayList<>();
                routeForSourceTable(tc, rrs, allNodeSet, schema, routeUnit, partNodeList);

                for (Pair<String, String> tn : ctx.getTables()) {
                    if (tn.equals(key)) {
                        continue;
                    } else if (CollectionUtil.containDuplicate(visitor.getSelectTableList(), dataSourceTc.getTableConfig().getName())) {
                        throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                    }
                    checkForEachTableInSQL(notShardingTableMap, partNodeList, tn, dataSourceTc, visitor.getRelationships(), tableAliasMap);
                }
            } else {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            }
        }
    }

    /**
     * check if the insert into a sharding-table and put a constant value in sharding-column
     * the scene change to route to single node
     *
     */
    private Collection<String> checkShardingKeyConstant(RouteTableConfigInfo dataSourceTc, RouteResultset rrs, ServerSchemaStatVisitor visitor,
                                                        String tableName, ShardingTableConfig tc, SchemaConfig schema) throws SQLException {
        RouteCalculateUnit singleRouteUnit = new RouteCalculateUnit();
        singleRouteUnit.addShardingExpr(new Pair<>(dataSourceTc.getSchema(), tableName), tc.getShardingColumn(), dataSourceTc.getValue());
        RouteResultset rrsTmp = RouterUtil.tryRouteForOneTable(schema, singleRouteUnit, tc.getName(), rrs, true);
        if (rrsTmp != null && rrsTmp.getNodes() != null) {
            if (rrsTmp.getNodes().length > 1) {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            }
            checkForSingleNodeTable(visitor, rrsTmp.getNodes()[0].getName(), rrs);
            return ImmutableList.of(rrsTmp.getNodes()[0].getName());
        } else {
            throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
        }
    }

    /**
     * check if the sql can be push down to the sharding table
     * The following conditions must be met
     * + the sharding-column data must come from a same-rule-sharding table
     * + the select can be ER route to all the dataNodes
     *
     */
    Collection<String> checkForShardingTable(ServerSchemaStatVisitor visitor, SQLSelect select, ServerConnection sc, RouteResultset rrs,
                                                       ShardingTableConfig tc, SchemaUtil.SchemaInfo schemaInfo, SQLStatement stmt, SchemaConfig schema) throws SQLException {
        //the insert table is a sharding table
        String tableName = schemaInfo.getTable();
        String schemaName = schema == null ? null : schema.getName();

        MySQLPlanNodeVisitor pvisitor = new MySQLPlanNodeVisitor(sc.getSchema(), sc.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), false, sc.getUsrVariables());
        pvisitor.visit(select);
        PlanNode node = pvisitor.getTableNode();
        node.setSql(rrs.getStatement());
        node.setUpFields();

        String partitionColumn = tc.getShardingColumn();
        int index = tryGetShardingColIndex(schemaInfo, stmt, partitionColumn);
        try {
            RouteTableConfigInfo dataSourceTc = node.findFieldSourceFromIndex(index);
            if (dataSourceTc == null) {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            } else if (dataSourceTc.getTableConfig() == null) {
                return checkShardingKeyConstant(dataSourceTc, rrs, visitor, tableName, tc, schema);
            } else if (dataSourceTc.getTableConfig() instanceof ShardingTableConfig && isSameSharding(tc, (ShardingTableConfig) (dataSourceTc.getTableConfig()))) {
                Map<String, String> tableAliasMap = getTableAliasMap(schemaName, visitor.getAliasMap());
                ctx.setRouteCalculateUnits(ConditionUtil.buildRouteCalculateUnits(visitor.getAllWhereUnit(), tableAliasMap, schemaName));

                Map<String, List<String>> notShardingTableMap = new HashMap<>();
                Set<String> allNodeSet = new HashSet<>();

                for (RouteCalculateUnit routeUnit : ctx.getRouteCalculateUnits()) {
                    checkForShardingSingleRouteUnit(notShardingTableMap, routeUnit, dataSourceTc, visitor, tableAliasMap, allNodeSet, schema, tc, rrs);
                }

                for (Map.Entry<String, List<String>> entry : notShardingTableMap.entrySet()) {
                    if (!entry.getValue().containsAll(allNodeSet)) {
                        throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                    }
                }
                return allNodeSet;
            } else {
                throw new Exception("not support");
            }
        } catch (Exception e) {
            throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
        }
    }

    /**
     * check if the sql can route to multi-node
     * when insert/replace multi-node global table
     * the conditions below must be met
     * + all the table must be mulit-node global(different node has the same data)
     * + all the dataNodes has all the table involved
     *
     */
    Collection<String> checkForMultiNodeGlobal(ServerSchemaStatVisitor visitor, GlobalTableConfig tc, SchemaConfig schema) throws SQLNonTransientException {
        //multi-Node global table
        List<String> mustContainList = tc.getShardingNodes();
        for (String sTable : visitor.getSelectTableList()) {
            BaseTableConfig stc = schema.getTables().get(sTable);
            if (stc != null && stc instanceof GlobalTableConfig) {
                if (!stc.getShardingNodes().containsAll(mustContainList)) {
                    throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                }
            } else {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            }
        }
        //route to all the dataNode the tc contain
        //RouterUtil.routeToMultiNode(false, rrs, mustContainList, true);
        return mustContainList;
    }


    Collection<String> checkForMultiNodeGlobal(List<SchemaUtil.SchemaInfo> schemaInfos) throws SQLNonTransientException {
        //multi-Node global table
        HashSet<String> mustContainList = new HashSet<>();
        for (SchemaUtil.SchemaInfo schemaInfo : schemaInfos) {
            BaseTableConfig tc = schemaInfo.getSchemaConfig().getTables().get(schemaInfo.getTable());
            if (mustContainList.size() == 0) {
                mustContainList.addAll(tc.getShardingNodes());
            } else if (tc.getShardingNodes().size() != mustContainList.size() ||
                    !mustContainList.containsAll(tc.getShardingNodes())) {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            }
        }
        //route to all the dataNode the tc contain
        //RouterUtil.routeToMultiNode(false, rrs, mustContainList, true);
        return mustContainList;
    }


    /**
     * check if the sql can be route to a single node
     * the check list below is:
     * + sharding table has condition to route to the dataNode
     * + nosharding/global table exists in that dataNode
     *
     */
    void checkForSingleNodeTable(ServerSchemaStatVisitor visitor, String dataNode, RouteResultset rrs) throws SQLNonTransientException {
        Set<Pair<String, String>> tablesSet = new HashSet<>(ctx.getTables());

        //loop for the tables & conditions
        for (RouteCalculateUnit routeUnit : ctx.getRouteCalculateUnits()) {
            Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = routeUnit.getTablesAndConditions();
            if (tablesAndConditions != null) {
                for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : tablesAndConditions.entrySet()) {
                    Pair<String, String> table = entry.getKey();
                    String sName = table.getKey();
                    String tName = table.getValue();
                    SchemaConfig tSchema = DbleServer.getInstance().getConfig().getSchemas().get(sName);
                    BaseTableConfig tConfig = tSchema.getTables().get(tName);
                    if (tConfig != null && tConfig instanceof ShardingTableConfig) {
                        if (!CollectionUtil.containDuplicate(visitor.getSelectTableList(), tName)) {
                            Set<String> tmpResultNodes = new HashSet<>();
                            tmpResultNodes.add(dataNode);
                            if (!RouterUtil.tryCalcNodeForShardingColumn(rrs, tmpResultNodes, tablesSet, entry, table, tConfig)) {
                                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                            }
                        } else {
                            throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                        }
                    }

                }
            }
        }

        for (Pair<String, String> table : tablesSet) {
            String sName = table.getKey();
            String tName = table.getValue();
            SchemaConfig tSchema = DbleServer.getInstance().getConfig().getSchemas().get(sName);
            BaseTableConfig tConfig = tSchema.getTables().get(tName);
            if (tConfig == null) {
                if (!tSchema.getShardingNode().equals(dataNode)) {
                    throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                }
            } else if ((tConfig instanceof SingleTableConfig)) {
                if (!tConfig.getShardingNodes().get(0).equals(dataNode)) {
                    throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                }
            } else if (tConfig instanceof GlobalTableConfig) {
                if (!tConfig.getShardingNodes().contains(dataNode)) {
                    throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                }
            } else {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            }
        }
    }


    Collection<String> checkForSingleNodeTable(RouteResultset rrs) throws SQLNonTransientException {
        Set<Pair<String, String>> tablesSet = new HashSet<>(ctx.getTables());
        Set<String> globalNodeSet = new HashSet<>();
        //loop for the tables & conditions
        for (RouteCalculateUnit routeUnit : ctx.getRouteCalculateUnits()) {
            Map<Pair<String, String>, Map<String, ColumnRoute>> tablesAndConditions = routeUnit.getTablesAndConditions();
            if (tablesAndConditions != null) {
                for (Map.Entry<Pair<String, String>, Map<String, ColumnRoute>> entry : tablesAndConditions.entrySet()) {
                    Pair<String, String> table = entry.getKey();
                    String sName = table.getKey();
                    String tName = table.getValue();
                    SchemaConfig tSchema = DbleServer.getInstance().getConfig().getSchemas().get(sName);
                    BaseTableConfig tConfig = tSchema.getTables().get(tName);
                    if (tConfig != null && tConfig instanceof ShardingTableConfig) {
                        if (!RouterUtil.tryCalcNodeForShardingColumn(rrs, globalNodeSet, tablesSet, entry, table, tConfig)) {
                            throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                        }
                    }

                }
            }
        }

        String dataNode = null;
        for (String x : globalNodeSet) {
            dataNode = x;
        }

        if (dataNode == null) {
            for (Pair<String, String> table : tablesSet) {
                String sName = table.getKey();
                String tName = table.getValue();
                SchemaConfig tSchema = DbleServer.getInstance().getConfig().getSchemas().get(sName);
                BaseTableConfig tConfig = tSchema.getTables().get(tName);
                if (tConfig.getShardingNodes().size() == 1) {
                    dataNode = tConfig.getShardingNodes().get(0);
                }
            }
        }

        for (Pair<String, String> table : tablesSet) {
            String sName = table.getKey();
            String tName = table.getValue();
            SchemaConfig tSchema = DbleServer.getInstance().getConfig().getSchemas().get(sName);
            BaseTableConfig tConfig = tSchema.getTables().get(tName);
            if (tConfig == null) {
                if (!tSchema.getShardingNode().equals(dataNode)) {
                    throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                }
            } else if (tConfig instanceof SingleTableConfig) {
                if (!tConfig.getShardingNodes().get(0).equals(dataNode)) {
                    throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
                }
            } else if (tConfig instanceof GlobalTableConfig) {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            } else {
                throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
            }
        }
        return globalNodeSet;
    }

    static RouteResultset routeByERParentColumn(RouteResultset rrs, ChildTableConfig tc, String joinColumnVal, SchemaUtil.SchemaInfo schemaInfo)
            throws SQLNonTransientException {
        if (tc.getDirectRouteTC() != null) {
            ColumnRoute columnRoute = new ColumnRoute(joinColumnVal);
            checkDefaultValues(joinColumnVal, tc.getName(), schemaInfo.getSchema(), tc.getJoinColumn());
            Set<String> shardingNodeSet = RouterUtil.ruleCalculate(rrs, tc.getDirectRouteTC(), columnRoute, false);
            if (shardingNodeSet.size() != 1) {
                throw new SQLNonTransientException("parent key can't find  valid data node ,expect 1 but found: " + shardingNodeSet.size());
            }
            String dn = shardingNodeSet.iterator().next();
            if (SQLJob.LOGGER.isDebugEnabled()) {
                SQLJob.LOGGER.debug("found partion node (using parent partition rule directly) for child table to insert  " + dn + " sql :" + rrs.getStatement());
            }
            return RouterUtil.routeToSingleNode(rrs, dn);
        }
        return null;
    }

    private static ArrayList<String> findAliasByMap(Map<String, String> tableAliasMap, String name) {
        ArrayList<String> x = new ArrayList<>();
        for (Map.Entry<String, String> entry : tableAliasMap.entrySet()) {
            if (entry.getValue().equalsIgnoreCase(name)) {
                x.add(entry.getKey());
            }
        }
        return x;
    }

    /**
     * check if the column is not null and the
     */
    static void checkDefaultValues(String columnValue, String tableName, String schema, String partitionColumn) throws SQLNonTransientException {
        if (columnValue == null || "null".equalsIgnoreCase(columnValue)) {
            TableMeta meta = ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schema, tableName);
            for (TableMeta.ColumnMeta columnMeta : meta.getColumns()) {
                if (!columnMeta.isCanNull()) {
                    if (columnMeta.getName().equalsIgnoreCase(partitionColumn)) {
                        String msg = "Sharding column can't be null when the table in MySQL column is not null";
                        LOGGER.info(msg);
                        throw new SQLNonTransientException(msg);
                    }
                }
            }
        }
    }

    private void changeSql(RouteResultset rrs) {
        String sql = rrs.getStatement();
        for (Pair<String, String> table : ctx.getTables()) {
            String tschemaName = table.getKey();
            sql = RouterUtil.removeSchema(sql, tschemaName);
        }
        rrs.setStatement(sql);
    }

    List<SchemaUtil.SchemaInfo> checkPrivilegeForModifyTable(ServerConnection sc, String schemaName, SQLStatement stmt, List<SQLExprTableSource> tableList) throws SQLException {
        List<SchemaUtil.SchemaInfo> schemaInfos = new ArrayList<>();
        for (SQLExprTableSource x : tableList) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(sc.getUser(), schemaName, x);
            if (!ShardingPrivileges.checkPrivilege(sc.getUserConfig(), schemaInfo.getSchema(), schemaInfo.getTable(), ShardingPrivileges.CheckType.UPDATE)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
                throw new SQLNonTransientException(msg);
            }
            schemaInfos.add(schemaInfo);
        }
        return schemaInfos;
    }


    void routeForModifySubQueryList(RouteResultset rrs, BaseTableConfig tc, ServerSchemaStatVisitor visitor, SchemaConfig schema) throws SQLNonTransientException {
        changeSql(rrs);

        Collection<String> routeShardingNodes;
        if (tc == null || tc.getShardingNodes().size() == 1) {
            for (SQLSelect subSql : visitor.getFirstClassSubQueryList()) {
                ServerSchemaStatVisitor subVisitor = new ServerSchemaStatVisitor();
                acceptVisitor(subSql, subVisitor);
                ctx.getTables().clear();
                Map<String, String> tableAliasMap = getTableAliasMap(schema.getName(), subVisitor.getAliasMap());
                ctx.setRouteCalculateUnits(ConditionUtil.buildRouteCalculateUnits(subVisitor.getAllWhereUnit(), tableAliasMap, schema.getName()));
                checkForSingleNodeTable(visitor, tc == null ? schema.getShardingNode() : tc.getShardingNodes().get(0), rrs);
            }
            //set value for route result
            routeShardingNodes = ImmutableList.of(tc == null ? schema.getShardingNode() : tc.getShardingNodes().get(0));
        } else if (tc instanceof GlobalTableConfig) {
            routeShardingNodes = checkForMultiNodeGlobal(visitor, (GlobalTableConfig) tc, schema);
        } else {
            throw new SQLNonTransientException(MODIFY_SQL_NOT_SUPPORT_MESSAGE);
        }

        RouterUtil.routeToMultiNode(false, rrs, routeShardingNodes, true);
        rrs.setFinishedRoute(true);
    }

    private boolean isSameSharding(ShardingTableConfig config1, ShardingTableConfig config2) {
        if (!config1.getFunction().getAlias().equals(config2.getFunction().getAlias())) {
            return false;
        }
        if (config1.getShardingNodes().size() != config2.getShardingNodes().size()) {
            return false;
        }
        for (int i = 0; i < config1.getShardingNodes().size(); i++) {
            if (!config1.getShardingNodes().get(i).equals(config2.getShardingNodes().get(i))) {
                return false;
            }
        }
        return true;
    }
}
