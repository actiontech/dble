/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.HintNestLoopHelper;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.plan.node.JoinNode.Strategy;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.base.Strings;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public class JoinStrategyChooser {
    private Map<String, PlanNode> nodeMap;
    private JoinNode jn;
    private HintNestLoopHelper hintNestLoopHelper;

    public JoinStrategyChooser(JoinNode jn) {
        this.jn = jn;
        nodeMap = new HashMap<>();
        hintNestLoopHelper = new HintNestLoopHelper();
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
        if (jn.getLeftNode().type() != PlanNode.PlanNodeType.TABLE || jn.getRightNode().type() != PlanNode.PlanNodeType.TABLE) {
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

        boolean buildResult = buildNodeMap(jn);
        if (!buildResult) {
            return;
        }
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
        buildNestLoop(joinNode, rightNode, true);
        if (leftNode instanceof JoinNode) {
            traverseNode((JoinNode) leftNode);
        } else if (isSmallTable((TableNode) rightNode)) {
            buildNestLoop(joinNode, leftNode, joinNode.isInnerJoin());
        }
    }

    private void buildNestLoop(JoinNode joinNode, PlanNode node, boolean innerJoin) {
        if (isSmallTable((TableNode) node) || canDoAsMerge(joinNode) || !innerJoin) {
            return;
        }
        joinNode.setStrategy(JoinNode.Strategy.ALWAYS_NEST_LOOP);
        PlanNode dependedNode = findDependedNode(node, innerJoin);
        if (Objects.isNull(dependedNode)) {
            return;
        }
        handlerDependedNode(dependedNode, node);
        node.setNestLoopFilters(new ArrayList<>());
        node.setNestLoopDependNode(dependedNode);
    }

    private String getTableName(TableNode node) {
        String alias = node.getAlias();
        if (!Strings.isNullOrEmpty(alias)) {
            return alias;
        }
        return node.getTableName();
    }

    private PlanNode findDependedNode(PlanNode node, boolean innerJoin) {
        JoinNode joinNode = (JoinNode) node.getParent();
        PlanNode firstNode = null;
        List<ItemFuncEqual> joinFilter = joinNode.getJoinFilter();
        for (ItemFuncEqual itemFuncEqual : joinFilter) {
            List<Item> arguments = itemFuncEqual.arguments();
            Item item = arguments.stream().filter(argument -> !StringUtil.equals(getTableName((TableNode) node), argument.getTableName())).findFirst().get();
            PlanNode dependedNode = nodeMap.get(item.getTableName());
            if (Objects.isNull(firstNode)) {
                firstNode = dependedNode;
            }
            if (isSmallTable((TableNode) dependedNode) && innerJoin) {
                return dependedNode;
            }
        }
        return firstNode;
    }

    @Nullable
    private void handlerDependedNode(PlanNode dependedNode, PlanNode node) {
        setNestLoopDependOnNodeList(dependedNode, node);
        PlanNode parent = dependedNode.getParent();
        setNestLoopDependOnNodeList(dependedNode, node);
        boolean isCurrentNode = true;
        while (Objects.nonNull(parent)) {
            if (!(parent instanceof JoinNode) || !canDoAsMerge((JoinNode) parent)) {
                break;
            }
            dependedNode = parent;
            isCurrentNode = false;
            parent = parent.getParent();
        }
        if (!isCurrentNode) {
            setNestLoopDependOnNodeList(dependedNode, node);
        }
    }

    private void setNestLoopDependOnNodeList(PlanNode dependedNode, PlanNode node) {
        List<PlanNode> nodeList = Optional.ofNullable(dependedNode.getNestLoopDependOnNodeList()).orElse(new ArrayList<>());
        nodeList.add(nodeList.size(), node);
        dependedNode.setNestLoopDependOnNodeList(nodeList);
    }

    private boolean buildNodeMap(JoinNode joinNode) {
        PlanNode leftNode = joinNode.getLeftNode();
        PlanNode rightNode = joinNode.getRightNode();
        if (jn.isNotIn() || jn.getJoinFilter().isEmpty()) {
            return false;
        }
        if ((leftNode.type() != PlanNode.PlanNodeType.TABLE && !(leftNode instanceof JoinNode)) || rightNode.type() != PlanNode.PlanNodeType.TABLE) {
            return false;
        }
        if (leftNode instanceof JoinNode) {
            return buildNodeMap((JoinNode) leftNode);
        } else {
            ((TableNode) leftNode).setHintNestLoopHelper(hintNestLoopHelper);
            nodeMap.put(getTableName((TableNode) leftNode), leftNode);
        }
        ((TableNode) rightNode).setHintNestLoopHelper(hintNestLoopHelper);
        nodeMap.put(getTableName((TableNode) rightNode), rightNode);
        return true;
    }

    public boolean canDoAsMerge(JoinNode joinNode) {
        return PlanUtil.isGlobalOrER(joinNode);
    }


}
