/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.JoinNode.Strategy;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.PlanUtil;

import java.util.*;

public class JoinStrategyChooser {
    private Map<String, PlanNode> nodeMap;
    private JoinNode jn;

    public JoinStrategyChooser(JoinNode jn) {
        this.jn = jn;
        nodeMap = new HashMap<>();
    }

    public void tryNestLoop(boolean always) {
        if (always) {
            unConditionNestLoop();
        } else {
            conditionNestLoop();
        }
    }

    /**
     * conditionNestLoop
     *
     * @return boolean true:join can use the nest loop optimization,will not try to optimizer join's child
     * false:join can't use the nest loop optimization,try to optimizer join's child
     */
    public boolean conditionNestLoop() {
        if (jn.getLeftNode().type() != PlanNode.PlanNodeType.TABLE && jn.getRightNode().type() != PlanNode.PlanNodeType.TABLE) {
            return false;
        }
        if (jn.isNotIn() || jn.getJoinFilter().isEmpty()) {
            return false;
        }
        if (jn.isInnerJoin()) {
            return tryInnerJoinNestLoop();
        } else if (jn.getLeftOuter()) {
            return tryLeftJoinNestLoop();
        } else {
            return false;
        }
    }

    /**
     * @return
     */
    private boolean tryInnerJoinNestLoop() {
        TableNode tnLeft = (TableNode) jn.getLeftNode();
        TableNode tnRight = (TableNode) jn.getRightNode();
        boolean isLeftSmall = isSmallTable(tnLeft);
        boolean isRightSmall = isSmallTable(tnRight);
        if (isLeftSmall && isRightSmall)
            return false;
        else if (!isLeftSmall && !isRightSmall)
            return false;
        else {
            handleNestLoopStrategy(isLeftSmall);
            return true;
        }
    }

    /**
     * @return
     */
    private boolean tryLeftJoinNestLoop() {
        TableNode tnLeft = (TableNode) jn.getLeftNode();
        TableNode tnRight = (TableNode) jn.getRightNode();
        // left join and only left node has where filter
        if (isSmallTable(tnLeft) && !isSmallTable(tnRight)) {
            handleNestLoopStrategy(true);
            return true;
        } else {
            return false;
        }
    }

    private void handleNestLoopStrategy(boolean isLeftSmall) {
        jn.setStrategy(Strategy.NESTLOOP);
        TableNode tnLeft = (TableNode) jn.getLeftNode();
        TableNode tnRight = (TableNode) jn.getRightNode();
        TableNode tnBig = isLeftSmall ? tnRight : tnLeft;
        tnBig.setNestLoopFilters(new ArrayList<Item>());
    }

    /**
     * the table contains where is small table now
     *
     * @param tn
     * @return
     */
    private boolean isSmallTable(TableNode tn) {
        return tn.getWhereFilter() != null;
    }

    public void unConditionNestLoop() {
        buildNodeMap(jn);
        traverseNode(jn);
    }

    private boolean checkCondition(JoinNode joinNode) {
        if (joinNode.isNotIn() || joinNode.getJoinFilter().isEmpty()) {
            return false;
        }
        PlanNode leftNode = joinNode.getLeftNode();
        PlanNode rightNode = joinNode.getRightNode();
        if (!((leftNode instanceof JoinNode || leftNode instanceof TableNode) && rightNode instanceof TableNode)) {
            return false;
        }
        return true;
    }

    private void traverseNode(JoinNode joinNode) {
        if (!checkCondition(joinNode)) {
            return;
        }
        PlanNode leftNode = joinNode.getLeftNode();
        PlanNode rightNode = joinNode.getRightNode();
        buildNestLoop(joinNode, rightNode);
        if (leftNode instanceof JoinNode) {
            traverseNode((JoinNode) leftNode);
        }
    }

    private void buildNestLoop(JoinNode joinNode, PlanNode node) {
        joinNode.setStrategy(JoinNode.Strategy.NEW_NEST_LOOP);
        node.setNestLoopFilters(new ArrayList<>());
        node.setNestLoopDependNode(findDependNode(node));
    }

    private PlanNode findDependNode(PlanNode node) {
        JoinNode joinNode = (JoinNode) node.getParent();
        String firstTableName = null;
        List<ItemFuncEqual> joinFilter = joinNode.getJoinFilter();
        for (ItemFuncEqual itemFuncEqual : joinFilter) {
            List<Item> arguments = itemFuncEqual.arguments();
            String tableName = arguments.get(0).getTableName();
            firstTableName = Optional.ofNullable(firstTableName).orElse(tableName);
            JoinNode dependNodeParent = (JoinNode) nodeMap.get(tableName).getParent();
            if (canDoAsMerge(dependNodeParent)) {
                return dependNodeParent;
            }
        }
        return nodeMap.get(firstTableName);
    }

    private void buildNodeMap(JoinNode joinNode) {
        PlanNode leftNode = joinNode.getLeftNode();
        PlanNode rightNode = joinNode.getRightNode();
        if (leftNode instanceof JoinNode) {
            buildNodeMap((JoinNode) leftNode);
        } else {
            nodeMap.put(leftNode.getAlias(), leftNode);
            nodeMap.put(((TableNode) leftNode).getTableName(), leftNode);
        }
        nodeMap.put(rightNode.getAlias(), rightNode);
        nodeMap.put(((TableNode) rightNode).getTableName(), leftNode);
    }

    public boolean canDoAsMerge(JoinNode joinNode) {
        return PlanUtil.isGlobalOrER(joinNode);
    }


}
