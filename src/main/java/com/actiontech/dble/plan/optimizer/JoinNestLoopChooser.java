package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.backend.mysql.nio.handler.builder.HintNestLoopHelper;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.StringUtil;
import com.google.common.collect.Lists;

import java.util.*;

public class JoinNestLoopChooser {
    private Map<String, PlanNode> nodeMap;
    private Map<String, List<String>> nodeDependMap;
    private Map<String, List<HintPlanNodeGroup>> hintDependMap;
    private JoinNode jn;
    private HintPlanInfo hintPlanInfo;
    private HintNestLoopHelper hintNestLoopHelper;

    public JoinNestLoopChooser(JoinNode joinNode, HintPlanInfo hintPlanInfo) {
        this.jn = joinNode;
        this.hintPlanInfo = hintPlanInfo;
        hintDependMap = new HashMap<>();
        nodeMap = new HashMap<>();
        nodeDependMap = new HashMap<>();
        hintNestLoopHelper = new HintNestLoopHelper();
    }

    public void tryNestLoop() throws MySQLOutPutException {
        buildNode(jn);
        buildHintDependency();
        buildNestLoop();
    }

    private void buildNodeMap(PlanNode planNode) throws MySQLOutPutException {
        if (planNode instanceof JoinNode) {
            ((JoinNode) planNode).setStrategy(JoinNode.Strategy.HINT_NEST_LOOP);
            buildNode((JoinNode) planNode);
        } else if (planNode instanceof TableNode) {
            String alias = planNode.getAlias();
            Optional.ofNullable(alias).orElseThrow(() -> new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "table " + ((TableNode) planNode).getTableName() + " alias can not be null!"));
            ((TableNode) planNode).setHintNestLoopHelper(hintNestLoopHelper);
            nodeDependMap.put(alias, Lists.newArrayList());
            nodeMap.put(alias, planNode);
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "hintPlan not support");
        }
    }

    private void buildNode(JoinNode joinNode) throws MySQLOutPutException {
        if (joinNode.isNotIn()) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "sql not support nestLoop");
        }
        joinNode.setStrategy(JoinNode.Strategy.HINT_NEST_LOOP);
        PlanNode leftNode = joinNode.getLeftNode();
        PlanNode rightNode = joinNode.getRightNode();
        buildNodeMap(leftNode);
        buildNodeMap(rightNode);
    }

    private void buildNestLoop() {
        nodeDependMap.forEach((alias, v) -> {
            if (!v.isEmpty()) {
                String dependName = v.get(0);
                PlanNode currentNode = nodeMap.get(alias);
                PlanNode dependNode = nodeMap.get(dependName);
                List<PlanNode> nodeList = Optional.ofNullable(dependNode.getNestLoopDependOnNodeList()).orElse(new ArrayList<>());
                nodeList.add(nodeList.size(), currentNode);
                dependNode.setNestLoopDependOnNodeList(nodeList);
                currentNode.setNestLoopFilters(new ArrayList<>());
                currentNode.setNestLoopDependNode(dependNode);
                buildERNestLoop(dependNode, currentNode);
            }
        });
    }

    private void buildERNestLoop(PlanNode dependNode, PlanNode currentNode) {
        PlanNode parent = dependNode.getParent();
        if (Objects.nonNull(parent) && parent instanceof JoinNode && canDoAsMerge((JoinNode) parent)) {
            List<PlanNode> nodeList = Optional.ofNullable(parent.getNestLoopDependOnNodeList()).orElse(new ArrayList<>());
            nodeList.add(nodeList.size(), currentNode);
            parent.setNestLoopDependOnNodeList(nodeList);
            buildERNestLoop(parent, currentNode);
        }
    }

    private void checkErCondition(HintPlanNodeGroup group) {
        if (group.getType() == HintPlanNodeGroup.Type.ER) {
            List<HintPlanNode> nodes = group.getNodes();
            for (HintPlanNode node : nodes) {
                String alias = node.getName();
                JoinNode parent = (JoinNode) nodeMap.get(alias).getParent();
                if (!canDoAsMerge(parent)) {
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "hint explain build failures! check ER condition");
                }
            }
        }
    }

    private void buildHintDependency() {
        List<HintPlanNodeGroup> groups = hintPlanInfo.getGroups();
        int nodeSize = 0;
        HintPlanNodeGroup lastGroup = null;
        LinkedList<HintPlanNodeGroup> groupList = new LinkedList<>();
        for (HintPlanNodeGroup group : groups) {
            List<HintPlanNode> nodes = group.getNodes();
            nodeSize += nodes.size();
            checkErCondition(group);
            checkAndOrCondition(group);
            if (lastGroup != null) {
                for (HintPlanNode node : nodes) {
                    hintDependMap.put(node.getName(), Lists.newArrayList(groupList));
                }
            }
            groupList.addFirst(group);
            lastGroup = group;
        }
        if (nodeSize != nodeMap.size()) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "the number of tables in the hint plan and the actual SQL varies");
        }
        hintAndCheck();
    }

    private void hintAndCheck() {
        hintDependMap.forEach((k, v) -> {
            PlanNode currentNode = nodeMap.get(k);
            for (HintPlanNodeGroup hintPlanNodeGroup : v) {
                List<HintPlanNode> nodes = hintPlanNodeGroup.getNodes();
                for (HintPlanNode node : nodes) {
                    PlanNode dependNode = nodeMap.get(node.getName());
                    boolean result = dependencyHelper(dependNode, currentNode, currentNode);
                    if (result) {
                        nodeDependMap.get(currentNode.getAlias()).add(dependNode.getAlias());
                        return;
                    }
                }
            }
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "hint explain build failures! check table " + currentNode.getAlias() + " & condition");
        });
    }

    private boolean addSrcNodeItem(PlanNode dependNode, PlanNode currentNode, PlanNode srcNode) {
        JoinNode dependNodeParent = (JoinNode) dependNode.getParent();
        return addItem(dependNodeParent.getJoinFilter(), (TableNode) currentNode, srcNode);
    }

    private boolean dependencyHelper(PlanNode dependNode, PlanNode currentNode, PlanNode srcNode) {
        //must have the on condition
        if (addSrcNodeItem(dependNode, currentNode, srcNode) || addSrcNodeItem(currentNode, dependNode, srcNode)) {
            //reasonable join relationship
            return traverseNode((JoinNode) dependNode.getParent(), currentNode, true) || traverseNode((JoinNode) currentNode.getParent(), dependNode, false);
        }
        return false;
    }

    private boolean traverseNode(JoinNode joinNode, PlanNode currentNode, boolean flip) {
        if (Objects.isNull(joinNode)) {
            return false;
        }
        PlanNode leftNode = joinNode.getLeftNode();
        PlanNode rightNode = joinNode.getRightNode();
        if (!flip) {
            leftNode = joinNode.getRightNode();
            rightNode = joinNode.getLeftNode();
        }
        //the right node can confirm that it can execute,the left node must be identified as inner
        if (Objects.equals(rightNode, currentNode) || (Objects.equals(leftNode, currentNode) && joinNode.isInnerJoin())) {
            return true;
        }
        return traverseNode((JoinNode) joinNode.getParent(), currentNode, flip);

    }

    private boolean addItem(List<ItemFuncEqual> joinFilter, TableNode node, PlanNode srcNode) {
        String alias = node.getAlias();
        for (ItemFuncEqual itemFuncEqual : joinFilter) {
            List<Item> arguments = itemFuncEqual.arguments();
            Optional<Item> first = arguments.stream().filter(argument -> StringUtil.equals(alias, argument.getTableName())).findFirst();
            if (first.isPresent()) {
                node.getHintNestLoopHelper().getItemMap().put(srcNode, createPair(arguments, srcNode));
                return true;
            }
        }
        return false;
    }

    private Pair createPair(List<Item> arguments, PlanNode node) {
        String alias = node.getAlias();
        Item keySource = arguments.get(0);
        Item keyToPass = arguments.get(1);
        if (StringUtil.equals(arguments.get(0).getTableName(), alias)) {
            return new Pair(keyToPass, keySource);
        } else {
            return new Pair(keySource, keyToPass);
        }
    }

    private void checkAndOrCondition(HintPlanNodeGroup group) {
        if (group.getType() != HintPlanNodeGroup.Type.ER) {
            List<HintPlanNode> nodes = group.getNodes();
            for (HintPlanNode node : nodes) {
                String alias = node.getName();
                Optional.ofNullable(nodeMap.get(alias)).orElseThrow(() -> new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "hint explain build failures! check table alias = " + alias));
                JoinNode parent = (JoinNode) nodeMap.get(alias).getParent();
                if (canDoAsMerge(parent)) {
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "hint explain build failures! check table " + alias + " & or | condition");
                }
            }
        }
    }

    public boolean canDoAsMerge(JoinNode joinNode) {
        return PlanUtil.isGlobalOrER(joinNode);
    }
}
