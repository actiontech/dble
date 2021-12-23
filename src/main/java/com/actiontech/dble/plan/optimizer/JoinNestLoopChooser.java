package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.google.common.base.Strings;

import java.util.*;
import java.util.stream.Collectors;

public class JoinNestLoopChooser {
    private Map<String, PlanNode> nodeMap;
    private Map<String, HintPlanNodeGroup> hintDependMap;
    private JoinNode jn;
    private HintPlanInfo hintPlanInfo;

    public JoinNestLoopChooser(JoinNode joinNode, HintPlanInfo hintPlanInfo) {
        this.jn = joinNode;
        this.hintPlanInfo = hintPlanInfo;
        hintDependMap = new HashMap<>();
        nodeMap = new HashMap<>();
    }

    public void tryNestLoop() throws MySQLOutPutException {
        buildNodeMap(jn);
        buildHintDependency();
        traverseNode(jn);
    }

    private void buildNodeMap(JoinNode joinNode) {
        PlanNode leftNode = joinNode.getLeftNode();
        PlanNode rightNode = joinNode.getRightNode();
        if (leftNode instanceof JoinNode) {
            buildNodeMap((JoinNode) leftNode);
        } else {
            nodeMap.put(leftNode.getAlias(), leftNode);
        }
        nodeMap.put(rightNode.getAlias(), rightNode);
    }

    private void checkOnConditions(HintPlanNodeGroup hintPlanNodeGroup, PlanNode node) {
        JoinNode joinNode = (JoinNode) node.getParent();
        List<ItemFuncEqual> joinFilter = joinNode.getJoinFilter();
        List<HintPlanNode> nodes = hintPlanNodeGroup.getNodes();
        Set<String> nodeNameSet = nodes.stream().map(HintPlanNode::getName).collect(Collectors.toSet());
        for (ItemFuncEqual itemFuncEqual : joinFilter) {
            List<Item> arguments = itemFuncEqual.arguments();
            arguments.stream().filter(argument -> nodeNameSet.contains(argument.getTableName())).findFirst().orElseThrow(() ->
                    new MySQLOutPutException(ErrorCode.ER_HINT_EXPLAIN_PLAN, "", "hint explain build failures!"));
        }
    }

    private void traverseNode(JoinNode joinNode) {
        PlanNode leftNode = joinNode.getLeftNode();
        PlanNode rightNode = joinNode.getRightNode();

        if (joinNode.isNotIn() || !((leftNode instanceof JoinNode || leftNode instanceof TableNode) && rightNode instanceof TableNode)) {
            throw new MySQLOutPutException(ErrorCode.ER_HINT_EXPLAIN_PLAN, "", "hint explain build failures!");
        }
        buildNestLoop(joinNode, rightNode);
        if (leftNode instanceof JoinNode) {
            traverseNode((JoinNode) leftNode);
        } else {
            if (joinNode.isLeftOuterJoin()) {
                throw new MySQLOutPutException(ErrorCode.ER_HINT_EXPLAIN_PLAN, "", "hint explain build failures!");
            }
            if (joinNode.isInnerJoin()) {
                buildNestLoop(joinNode, leftNode);
            }

        }
    }

    private void buildNestLoop(JoinNode joinNode, PlanNode node) {
        String alias = node.getAlias();
        if (Strings.isNullOrEmpty(alias)) {
            throw new MySQLOutPutException(ErrorCode.ER_HINT_EXPLAIN_PLAN, "", "table alias can not be null!");
        }
        HintPlanNodeGroup hintPlanNodeGroup = hintDependMap.get(alias);
        if (Objects.nonNull(hintPlanNodeGroup)) {
            checkOnConditions(hintPlanNodeGroup, node);
        }
        if (hintDependMap.containsKey(alias)) {
            joinNode.setStrategy(JoinNode.Strategy.HINT_NEST_LOOP);
            node.setNestLoopFilters(new ArrayList<>());
            node.setNestLoopDependNode(findDependNode(node));
        }
    }

    private PlanNode findDependNode(PlanNode node) {
        JoinNode joinNode = (JoinNode) node.getParent();
        String firstTableName = null;
        List<ItemFuncEqual> joinFilter = joinNode.getJoinFilter();
        for (ItemFuncEqual itemFuncEqual : joinFilter) {
            List<Item> arguments = itemFuncEqual.arguments();
            String tableName = arguments.get(0).getTableName();
            firstTableName = Optional.ofNullable(firstTableName).orElse(tableName);
            HintPlanNodeGroup group = hintDependMap.get(node.getAlias());
            if (group.getType() == HintPlanNodeGroup.Type.ER) {
                return nodeMap.get(tableName).getParent();
            }
            if (!nodeMap.containsKey(tableName)) {
                return nodeMap.get(tableName);
            }
        }
        return nodeMap.get(firstTableName);
    }

    private void checkErCondition(HintPlanNodeGroup group) {
        if (group.getType() == HintPlanNodeGroup.Type.ER) {
            List<HintPlanNode> nodes = group.getNodes();
            for (HintPlanNode node : nodes) {
                String alias = node.getName();
                JoinNode parent = (JoinNode) nodeMap.get(alias).getParent();
                if (!canDoAsMerge(parent)) {
                    throw new MySQLOutPutException(ErrorCode.ER_HINT_EXPLAIN_PLAN, "", "hint explain build failures! check ER or AND or OR condition");
                }
            }
        }

    }

    private void buildHintDependency() {
        List<HintPlanNodeGroup> groups = hintPlanInfo.getGroups();
        HintPlanNodeGroup lastGroup = null;
        for (HintPlanNodeGroup group : groups) {
            List<HintPlanNode> nodes = group.getNodes();
            checkErCondition(group);
            checkAndOrCondition(group);
            if (lastGroup != null) {
                for (HintPlanNode node : nodes) {
                    hintDependMap.put(node.getName(), lastGroup);
                }
            }
            lastGroup = group;
        }
    }

    private void checkAndOrCondition(HintPlanNodeGroup group) {
        if (group.getType() != HintPlanNodeGroup.Type.ER) {
            List<HintPlanNode> nodes = group.getNodes();
            for (HintPlanNode node : nodes) {
                String alias = node.getName();
                Optional.ofNullable(nodeMap.get(alias)).orElseThrow(() -> new MySQLOutPutException(ErrorCode.ER_HINT_EXPLAIN_PLAN, "", "hint explain build failures! check table alias = " + alias));
                JoinNode parent = (JoinNode) nodeMap.get(alias).getParent();
                if (canDoAsMerge(parent)) {
                    throw new MySQLOutPutException(ErrorCode.ER_HINT_EXPLAIN_PLAN, "", "hint explain build failures! check ER or AND or OR condition");
                }
            }
        }
    }

    public boolean canDoAsMerge(JoinNode joinNode) {
        return PlanUtil.isGlobalOrER(joinNode);
    }
}
