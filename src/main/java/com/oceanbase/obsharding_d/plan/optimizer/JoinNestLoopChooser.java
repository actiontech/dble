/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.backend.mysql.nio.handler.builder.HintNestLoopHelper;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.server.parser.HintPlanParse;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.Lists;

import java.util.*;

public class JoinNestLoopChooser {
    private Map<String, PlanNode> nodeMap;
    private Map<String, List<String>> nodeDependMap;
    private JoinNode jn;
    private HintPlanInfo hintPlanInfo;
    private HintNestLoopHelper hintNestLoopHelper;

    public JoinNestLoopChooser(JoinNode joinNode, HintPlanInfo hintPlanInfo) {
        this.jn = joinNode;
        this.hintPlanInfo = hintPlanInfo;
        nodeMap = new HashMap<>();
        nodeDependMap = new HashMap<>();
        hintNestLoopHelper = new HintNestLoopHelper();
    }

    public void tryNestLoop() throws MySQLOutPutException {
        buildNode(jn);
        checkHintDependency();
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
                HashMap<String, Set<HintPlanNode>> dependMap = hintPlanInfo.getDependMap();
                Set<HintPlanNode> hintPlanNodes = dependMap.get(alias);
                LinkedHashMap<String, HintPlanParse.Type> hintPlanNodeMap = hintPlanInfo.getHintPlanNodeMap();
                String dependName = hintPlanNodes.iterator().next().getName();
                if (hintPlanNodes.size() > 1 && !StringUtil.equals(dependName, v.get(0)) && hintPlanNodeMap.get(dependName) == HintPlanParse.Type.AND) {
                    PlanNode currentNode = nodeMap.get(alias);
                    PlanNode dependNode = nodeMap.get(dependName).getParent();
                    TableNode fakeDependNode = (TableNode) nodeMap.get(v.get(0));
                    List<PlanNode> nodeList = Optional.ofNullable(dependNode.getNestLoopDependOnNodeList()).orElse(new ArrayList<>());
                    nodeList.add(nodeList.size(), currentNode);
                    dependNode.setNestLoopDependOnNodeList(nodeList);
                    fakeDependNode.getHintNestLoopHelper().getFakeDependSet().add(currentNode);
                }

                dependName = v.get(0);
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

    private void checkErCondition(HashMap<String, Set<HintPlanNode>> erMap) {
        erMap.forEach((k, v) -> {
            Set<HintPlanNode> nodes = v;
            for (HintPlanNode node : nodes) {
                String alias = node.getName();
                JoinNode parent = (JoinNode) nodeMap.get(alias).getParent();
                if (!canDoAsMerge(parent)) {
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "hint explain build failures! check ER condition");
                }
            }
        });
    }

    private void checkHintDependency() {
        LinkedHashMap<String, HintPlanParse.Type> hintPlanNodeMap = hintPlanInfo.getHintPlanNodeMap();
        HashMap<String, Set<HintPlanNode>> erMap = hintPlanInfo.getErMap();
        checkErCondition(erMap);
        if (hintPlanInfo.nodeSize() != nodeMap.size()) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "the number of tables in the hint plan and the actual SQL varies");
        }
        hintPlanNodeMap.forEach((k, v) -> {
            if (v != HintPlanParse.Type.ER) {
                checkAndOrCondition(k);
            }
        });
        hintAndCheck();
    }

    private void hintAndCheck() {
        HashMap<String, Set<HintPlanNode>> dependMap = hintPlanInfo.getDependMap();
        dependMap.forEach((k, v) -> {
            PlanNode currentNode = nodeMap.get(k);
            Set<HintPlanNode> nodes = v;
            for (HintPlanNode node : nodes) {
                PlanNode dependNode = nodeMap.get(node.getName());
                boolean result = dependencyHelper(dependNode, currentNode, currentNode);
                if (result) {
                    //the verified dependency need to be put into the nodeDependMap
                    String alias = currentNode.getAlias();
                    List<String> dependList = Optional.ofNullable(nodeDependMap.get(alias)).orElse(Lists.newArrayList());
                    dependList.add(dependNode.getAlias());
                    nodeDependMap.put(k, dependList);
                    return;
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

    private void checkAndOrCondition(String alias) {
        Optional.ofNullable(nodeMap.get(alias)).orElseThrow(() -> new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "hint explain build failures! check table alias = " + alias));
        JoinNode parent = (JoinNode) nodeMap.get(alias).getParent();
        if (canDoAsMerge(parent)) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "hint explain build failures! check table " + alias + " & or | condition");
        }
    }

    public boolean canDoAsMerge(JoinNode joinNode) {
        return PlanUtil.isGlobalOrER(joinNode);
    }
}
