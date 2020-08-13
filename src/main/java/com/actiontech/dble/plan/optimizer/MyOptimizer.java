/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.subquery.ItemSubQuery;
import com.actiontech.dble.plan.node.JoinInnerNode;
import com.actiontech.dble.plan.node.NoNameNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class MyOptimizer {
    private MyOptimizer() {
    }

    public static PlanNode optimize(PlanNode node) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-for-sql");
        TraceManager.log(ImmutableMap.of("plan-node", node), traceObject);
        try {
            // PreProcessor SubQuery ,transform in sub query to join
            node = SubQueryPreProcessor.optimize(node);
            updateReferredTableNodes(node);
            int existGlobal = checkGlobalTable(node, new HashSet<>());
            if (node.isExistView() || existGlobal != 1 || node.isWithSubQuery() || node.isContainsSubQuery() || !PlanUtil.hasNoFakeNode(node)) {
                // optimizer sub query [Derived Tables (Subqueries in the FROM Clause)]
                //node = SubQueryProcessor.optimize(node);
                // transform right join to left join
                node = JoinPreProcessor.optimize(node);

                //  filter expr which is always true/false
                node = FilterPreProcessor.optimize(node);
                //  push down the filter which may contains ER KEY
                node = FilterJoinColumnPusher.optimize(node);

                node = JoinERProcessor.optimize(node);

                if (existGlobal >= 0) {
                    node = GlobalTableProcessor.optimize(node);
                }
                //  push down filter
                node = FilterPusher.optimize(node);

                node = OrderByPusher.optimize(node);

                node = LimitPusher.optimize(node);

                node = SelectedProcessor.optimize(node);

                boolean useJoinStrategy = SystemConfig.getInstance().isUseJoinStrategy();
                if (useJoinStrategy) {
                    node = JoinStrategyProcessor.optimize(node);
                }
            }
            return node;
        } catch (MySQLOutPutException e) {
            LoggerFactory.getLogger(MyOptimizer.class).error(node.toString(), e);
            throw e;
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public static PlanNode managerOptimize(PlanNode node) {
        try {
            // PreProcessor SubQuery ,transform in sub query to join
            node = SubQueryPreProcessor.optimize(node);

            // transform right join to left join
            node = JoinPreProcessor.optimize(node);

            //  filter expr which is always true/false
            node = FilterPreProcessor.optimize(node);

            //  push down filter
            node = FilterPusher.optimize(node);

            node = OrderByPusher.optimize(node);

            node = LimitPusher.optimize(node);

            node = SelectedProcessor.optimize(node);

            return node;
        } catch (MySQLOutPutException e) {
            LoggerFactory.getLogger(MyOptimizer.class).error(node.toString(), e);
            throw e;
        }
    }

    private static List<TableNode> updateReferredTableNodes(PlanNode node) {
        List<TableNode> subTables = new ArrayList<>();
        for (PlanNode childNode : node.getChildren()) {
            List<TableNode> childSubTables = updateReferredTableNodes(childNode);
            node.getReferedTableNodes().addAll(childSubTables);
            subTables.addAll(childSubTables);
        }
        for (ItemSubQuery subQuery : node.getSubQueries()) {
            List<TableNode> childSubTables = subQuery.getPlanNode().getReferedTableNodes();
            node.getReferedTableNodes().addAll(childSubTables);
            subTables.addAll(childSubTables);
        }
        return subTables;
    }

    /**
     * existShardTable
     *
     * @param node
     * @return return 1 if it's all no name table or all global table node;
     * return -1 if all the table is not global table,need not global optimizer;
     * return 0 for other ,may need to global optimizer ;
     */
    public static int checkGlobalTable(PlanNode node, Set<String> resultShardingNodes) {
        if (node.isWithSubQuery()) {
            return 0;
        }
        Set<String> shardingNodes = null;
        boolean isAllGlobal = true;
        boolean isContainGlobal = false;
        for (TableNode tn : node.getReferedTableNodes()) {
            if (tn.getUnGlobalTableCount() == 0) {
                isContainGlobal = true;
                if (isAllGlobal) {
                    if (shardingNodes == null) {
                        shardingNodes = new HashSet<>();
                        shardingNodes.addAll(tn.getNoshardNode());
                    } else {
                        shardingNodes.retainAll(tn.getNoshardNode());
                    }
                } else {
                    return 0;
                }
            } else {
                isAllGlobal = false;
                if (isContainGlobal) {
                    return 0;
                }
            }
        }


        if (isAllGlobal) {
            if (shardingNodes == null) { // all nonamenode
                String db = SchemaUtil.getRandomDb();
                SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(db);
                node.setNoshardNode(schemaConfig.getAllShardingNodes());
                resultShardingNodes.addAll(schemaConfig.getAllShardingNodes());
                return 1;
            } else if (shardingNodes.size() > 0) { //all global table
                node.setNoshardNode(shardingNodes);
                resultShardingNodes.addAll(shardingNodes);
                String sql = node.getSql();
                for (TableNode tn : node.getReferedTableNodes()) {
                    sql = RouterUtil.removeSchema(sql, tn.getSchema());
                }
                node.setSql(sql);
                return 1;
            } else {
                return 0;
            }
        } else if (containsNoNameNode(node)) {
            return 0;
        }
        return -1;
    }

    private static boolean containsNoNameNode(PlanNode node) {
        if (node instanceof NoNameNode || node instanceof JoinInnerNode) {
            return true;
        }
        for (PlanNode child : node.getChildren()) {
            if (containsNoNameNode(child)) {
                return true;
            }
        }
        return false;
    }

}
