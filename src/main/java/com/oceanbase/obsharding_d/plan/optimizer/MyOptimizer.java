/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.subquery.ItemSubQuery;
import com.oceanbase.obsharding_d.plan.node.JoinInnerNode;
import com.oceanbase.obsharding_d.plan.node.NoNameNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.route.util.RouterUtil;
import com.oceanbase.obsharding_d.server.util.SchemaUtil;
import com.oceanbase.obsharding_d.singleton.TraceManager;
import com.google.common.collect.ImmutableMap;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class MyOptimizer {
    private MyOptimizer() {
    }

    public static PlanNode optimize(PlanNode node, @Nonnull HintPlanInfo hintPlanInfo) {

        TraceManager.TraceObject traceObject = TraceManager.threadTrace("optimize-for-sql");
        TraceManager.log(ImmutableMap.of("plan-node", node), traceObject);
        try {

            if (!hintPlanInfo.isIn2join()) {
                if (SystemConfig.getInstance().isInSubQueryTransformToJoin()) {
                    // PreProcessor SubQuery ,transform in sub query to join
                    node = SubQueryPreProcessor.optimize(node);
                } else {
                    SubQueryPreNoTransformProcessor.optimize(node);
                }
            } else {
                // PreProcessor SubQuery ,transform in sub query to join; '/*!OBsharding-D:plan=In2join*/ select...';
                node = SubQueryPreProcessor.optimize(node);
            }
            updateReferredTableNodes(node);
            int existGlobal = checkGlobalTable(node, new HashSet<>());
            if (node.type() == PlanNode.PlanNodeType.QUERY || node.isExistView() || existGlobal != 1 || node.isWithSubQuery() || node.isContainsSubQuery() || !PlanUtil.hasNoFakeNode(node)) {
                // optimizer sub query [Derived Tables (Subqueries in the FROM Clause)]
                //node = SubQueryProcessor.optimize(node);
                // transform right join to left join
                JoinPreProcessor.optimize(node);

                //  filter expr which is always true/false
                FilterPreProcessor.optimize(node);
                //  push down the filter which may contains ER KEY
                node = FilterJoinColumnPusher.optimize(node);


                if (SystemConfig.getInstance().isUseNewJoinOptimizer() || !hintPlanInfo.isZeroNode()) {
                    node = JoinProcessor.optimize(node, hintPlanInfo);
                } else {
                    node = JoinERProcessor.optimize(node);
                }
                if (existGlobal >= 0) {
                    GlobalTableProcessor.optimize(node);
                }
                //  push down filter
                node = FilterPusher.optimize(node);

                OrderByPusher.optimize(node);

                LimitPusher.optimize(node);

                node = SelectedProcessor.optimize(node);
                if (!hintPlanInfo.isZeroNode()) {
                    HintStrategyNestLoopProcessor.optimize(node, hintPlanInfo);
                } else {
                    JoinStrategyProcessor.chooser(node);
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
            JoinPreProcessor.optimize(node);

            //  filter expr which is always true/false
            FilterPreProcessor.optimize(node);

            //  push down filter
            node = FilterPusher.optimize(node);

            node = OrderByPusher.optimize(node);

            LimitPusher.optimize(node);

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
     * @param node PlanNode
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
                        shardingNodes = new HashSet<>(tn.getNoshardNode());
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
                SchemaConfig schemaConfig = OBsharding_DServer.getInstance().getConfig().getSchemas().get(db);
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
