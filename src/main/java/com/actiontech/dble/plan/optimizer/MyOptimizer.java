/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.subquery.ItemSubQuery;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.route.util.RouterUtil;
import com.actiontech.dble.server.util.SchemaUtil;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class MyOptimizer {
    private MyOptimizer() {
    }

    public static PlanNode optimize(PlanNode node) {

        try {
            // PreProcessor SubQuery ,transform in sub query to join
            node = SubQueryPreProcessor.optimize(node);
            updateReferedTableNodes(node);
            int existGlobal = checkGlobalTable(node, new HashSet<String>());
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

                boolean useJoinStrategy = DbleServer.getInstance().getConfig().getSystem().isUseJoinStrategy();
                if (useJoinStrategy) {
                    node = JoinStrategyProcessor.optimize(node);
                }
            }
            return node;
        } catch (MySQLOutPutException e) {
            LoggerFactory.getLogger(MyOptimizer.class).error(node.toString(), e);
            throw e;
        }
    }

    private static List<TableNode> updateReferedTableNodes(PlanNode node) {
        List<TableNode> subTables = new ArrayList<>();
        for (PlanNode childNode : node.getChildren()) {
            List<TableNode> childSubTables = updateReferedTableNodes(childNode);
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
    public static int checkGlobalTable(PlanNode node, Set<String> resultDataNodes) {
        if (node.isWithSubQuery()) {
            return 0;
        }
        Set<String> dataNodes = null;
        boolean isAllGlobal = true;
        boolean isContainGlobal = false;
        for (TableNode tn : node.getReferedTableNodes()) {
            if (tn.getUnGlobalTableCount() == 0) {
                isContainGlobal = true;
                if (isAllGlobal) {
                    if (dataNodes == null) {
                        dataNodes = new HashSet<>();
                        dataNodes.addAll(tn.getNoshardNode());
                    } else {
                        dataNodes.retainAll(tn.getNoshardNode());
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
            if (dataNodes == null) { // all nonamenode
                String db = SchemaUtil.getRandomDb();
                SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(db);
                node.setNoshardNode(schemaConfig.getAllDataNodes());
                resultDataNodes.addAll(schemaConfig.getAllDataNodes());
                return 1;
            } else if (dataNodes.size() > 0) { //all global table
                node.setNoshardNode(dataNodes);
                resultDataNodes.addAll(dataNodes);
                String sql = node.getSql();
                for (TableNode tn : node.getReferedTableNodes()) {
                    sql = RouterUtil.removeSchema(sql, tn.getSchema());
                }
                node.setSql(sql);
                return 1;
            } else {
                return 0;
            }
        }
        return -1;
    }


}
