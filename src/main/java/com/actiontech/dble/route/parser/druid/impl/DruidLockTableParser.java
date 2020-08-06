/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser.druid.impl;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.subquery.ItemSubQuery;
import com.actiontech.dble.plan.node.*;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.druid.ServerSchemaStatVisitor;

import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;

import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.*;

/**
 * lock tables [table] [writeDirectly|read]
 *
 * @author songdabin
 */
public class DruidLockTableParser extends DefaultDruidParser {
    @Override
    public SchemaConfig visitorParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, ServerSchemaStatVisitor visitor, ShardingService service, boolean isExplain)
            throws SQLException {
        MySqlLockTableStatement lockTableStat = (MySqlLockTableStatement) stmt;
        Map<String, Set<String>> shardingNodeToLocks = new HashMap<>();
        for (MySqlLockTableStatement.Item item : lockTableStat.getItems()) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.getSchemaInfo(service.getUser(), schema == null ? null : schema.getName(), item.getTableSource());

            String table = schemaInfo.getTable();
            String schemaName = schemaInfo.getSchema();
            SchemaConfig schemaConfig = schemaInfo.getSchemaConfig();
            BaseTableConfig tableConfig = schemaConfig.getTables().get(table);
            if (tableConfig != null) {
                handleConfigTable(shardingNodeToLocks, tableConfig, item.getTableSource().getAlias(), item.getLockType());
                continue;
            } else if (ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaName, table) != null || ProxyMeta.getInstance().getTmManager().getSyncView(schemaName, table) instanceof TableNode) {
                handleNoshardTable(shardingNodeToLocks, table, schemaConfig.getShardingNode(), item.getTableSource().getAlias(), item.getLockType());
                continue;
            } else if (ProxyMeta.getInstance().getTmManager().getSyncView(schemaName, table) instanceof QueryNode) {
                handleSingleViewLock(shardingNodeToLocks, ProxyMeta.getInstance().getTmManager().getSyncView(schemaName, table), item.getTableSource().getAlias(), item.getLockType(), schemaName);
                continue;
            }
            String msg = "Table '" + schemaConfig.getName() + "." + table + "' doesn't exist";
            LOGGER.info(msg);
            throw new SQLNonTransientException(msg);
        }

        Set<RouteResultsetNode> lockedNodes = new HashSet<>();
        if (service.isLocked()) {
            lockedNodes.addAll(service.getSession2().getTargetMap().keySet());
        }
        List<RouteResultsetNode> nodes = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : shardingNodeToLocks.entrySet()) {
            RouteResultsetNode node = new RouteResultsetNode(entry.getKey(), ServerParse.LOCK, " LOCK TABLES " + StringUtil.join(entry.getValue(), ","));
            nodes.add(node);
            lockedNodes.remove(node);
        }
        for (RouteResultsetNode toUnlockedNode : lockedNodes) {
            RouteResultsetNode node = new RouteResultsetNode(toUnlockedNode.getName(), ServerParse.UNLOCK, " UNLOCK TABLES ");
            nodes.add(node);
        }
        rrs.setNodes(nodes.toArray(new RouteResultsetNode[nodes.size()]));
        rrs.setFinishedRoute(true);
        return schema;
    }


    /**
     * handle single config table lock
     */
    private void handleConfigTable(Map<String, Set<String>> shardingNodeToLocks, BaseTableConfig tableConfig, String alias, MySqlLockTableStatement.LockType lockType) {
        List<String> shardingNodes = tableConfig.getShardingNodes();
        for (String shardingNode : shardingNodes) {
            StringBuilder sbItem = new StringBuilder(tableConfig.getName());
            if (alias != null) {
                sbItem.append(" as ");
                sbItem.append(alias);
            }
            sbItem.append(" ");
            sbItem.append(lockType);
            Set<String> locks = shardingNodeToLocks.computeIfAbsent(shardingNode, k -> new HashSet<>());
            locks.add(sbItem.toString());
        }
    }

    private void handleNoshardTable(Map<String, Set<String>> shardingNodeToLocks, String tableName, String shardingNode, String alias, MySqlLockTableStatement.LockType lockType) {
        StringBuilder sbItem = new StringBuilder(tableName);
        if (alias != null) {
            sbItem.append(" as ");
            sbItem.append(alias);
        }
        sbItem.append(" ");
        sbItem.append(lockType);
        Set<String> locks = shardingNodeToLocks.computeIfAbsent(shardingNode, k -> new HashSet<String>());
        locks.add(sbItem.toString());
    }

    private void handleSingleViewLock(Map<String, Set<String>> shardingNodeToLocks, PlanNode viewQuery, String alias, MySqlLockTableStatement.LockType lockType, String schemaName) throws SQLNonTransientException {
        Map<String, Set<String>> tableMap = new HashMap<>();
        findTableInPlanNode(tableMap, viewQuery, schemaName);
        for (Map.Entry<String, Set<String>> entry : tableMap.entrySet()) {
            SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(entry.getKey());
            for (String table : entry.getValue()) {
                BaseTableConfig tableConfig = schemaConfig.getTables().get(table);
                if (tableConfig != null) {
                    handleConfigTable(shardingNodeToLocks, tableConfig, alias == null ? null : "view_" + alias + "_" + table, lockType);
                } else if (ProxyMeta.getInstance().getTmManager().getSyncTableMeta(schemaConfig.getName(), table) != null) {
                    handleNoshardTable(shardingNodeToLocks, table, schemaConfig.getShardingNode(), alias == null ? null : "view_" + alias + "_" + table, lockType);
                } else {
                    String msg = "Table '" + schemaConfig.getName() + "." + table + "' doesn't exist";
                    LOGGER.info(msg);
                    throw new SQLNonTransientException(msg);
                }
            }
        }
        return;
    }


    private void findTableInPlanNode(Map<String, Set<String>> tableSet, PlanNode pnode, String schema) {
        if (pnode instanceof QueryNode) {
            findTableInPlanNode(tableSet, pnode.getChild(), schema);
        } else if (pnode instanceof JoinNode) {
            JoinNode jnode = (JoinNode) pnode;
            for (Item x : jnode.getColumnsSelected()) {
                findTableInItem(tableSet, x, schema);
            }
            for (PlanNode p : jnode.getChildren()) {
                findTableInPlanNode(tableSet, p, schema);
            }
            if (jnode.getWhereFilter() != null) {
                findTableInItem(tableSet, jnode.getWhereFilter(), schema);
            }
            if (jnode.getHavingFilter() != null) {
                findTableInItem(tableSet, jnode.getHavingFilter(), schema);
            }
        } else if (pnode instanceof NoNameNode) {
            NoNameNode nnode = (NoNameNode) pnode;
            for (Item x : nnode.getColumnsSelected()) {
                findTableInItem(tableSet, x, schema);
            }
        } else if (pnode instanceof JoinInnerNode) {
            JoinInnerNode jnode = (JoinInnerNode) pnode;
            for (Item x : jnode.getColumnsSelected()) {
                findTableInItem(tableSet, x, schema);
            }
            for (PlanNode p : jnode.getChildren()) {
                findTableInPlanNode(tableSet, p, schema);
            }
            if (jnode.getWhereFilter() != null) {
                findTableInItem(tableSet, jnode.getWhereFilter(), schema);
            }
            if (jnode.getHavingFilter() != null) {
                findTableInItem(tableSet, jnode.getHavingFilter(), schema);
            }
        } else if (pnode instanceof MergeNode) {
            MergeNode mn = (MergeNode) pnode;
            for (PlanNode p : mn.getChildren()) {
                findTableInPlanNode(tableSet, p, schema);
            }
        } else if (pnode instanceof TableNode) {
            TableNode tnode = (TableNode) pnode;
            if (tnode.getSchema() == null) {
                addTableToSet(tableSet, tnode.getSchema(), tnode.getTableName());
            } else {
                addTableToSet(tableSet, schema, tnode.getTableName());
            }
            if (tnode.getWhereFilter() != null) {
                findTableInItem(tableSet, tnode.getWhereFilter(), schema);
            }
            if (tnode.getHavingFilter() != null) {
                findTableInItem(tableSet, tnode.getHavingFilter(), schema);
            }
        }
    }

    private void findTableInItem(Map<String, Set<String>> tableSet, Item it, String schema) {
        if (it instanceof ItemSubQuery) {
            findTableInPlanNode(tableSet, ((ItemSubQuery) it).getPlanNode(), schema);
        } else {
            if (it.arguments() != null) {
                for (Item x : it.arguments()) {
                    findTableInItem(tableSet, x, schema);
                }
            }
        }
    }

    private void addTableToSet(Map<String, Set<String>> tableSet, String schema, String table) {
        if (tableSet.get(schema) == null) {
            Set<String> set = new HashSet<>();
            set.add(table);
            tableSet.put(schema, set);
        } else {
            tableSet.get(schema).add(table);
        }
    }
}
