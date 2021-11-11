/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.plan.util.FilterUtils;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.util.StringUtil;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.Map.Entry;

public class JoinChooser {

    private final List<JoinRelations> joinRelations = new ArrayList<>();
    private final List<PlanNode> joinUnits = new ArrayList<>();
    private final Set<PlanNode> dagNodes = new HashSet<>();
    private final Map<ERTable, Set<ERTable>> erRelations;
    private final JoinNode orgNode;
    private final Comparator<JoinRelationDag> defaultCmp = (o1, o2) -> {
        if (o1.relations.isERJoin && o2.relations.isERJoin) {
            if (o1.relations.isInner) { // both er，o1 inner
                return -1;
            } else if (o2.relations.isInner) { //both er，o1 not inner,o2 inner
                return 1;
            } else { // both er， left join
                return 0;
            }
        } else if (o1.relations.isERJoin) { // if o1 is not ER join, o1 is ER join, o1<>>o2
            return -1;
        } else if (o2.relations.isERJoin) { // if o1 is not ER join, o2 is ER join, o1>o2
            return 1;
        } else {
            // both o1,o2 are not ER join, global table should be first
            boolean o1Global = o1.node.getUnGlobalTableCount() == 0;
            boolean o2Global = o2.node.getUnGlobalTableCount() == 0;
            if (o1Global == o2Global) {
                if (o1.relations.isInner) { //  o1 inner
                    return -1;
                } else if (o2.relations.isInner) { //o1 not inner,o2 inner
                    return 1;
                } else {
                    return 0;
                }
            } else if (o1Global) {
                return -1;
            } else // if (o2Global) {
                return 1;
        }
    };

    public JoinChooser(JoinNode qtn, Map<ERTable, Set<ERTable>> erRelations) {
        this.orgNode = qtn;
        this.erRelations = erRelations;
    }

    public JoinChooser(JoinNode qtn) {
        this(qtn, DbleServer.getInstance().getConfig().getErRelations());
    }

    public JoinNode optimize() {
        if (erRelations == null) {
            return orgNode;
        }
        return innerJoinOptimizer(orgNode.getCharsetIndex());
    }


    /**
     * inner join's ER, rebuild inner join's unit
     */
    private JoinNode innerJoinOptimizer(int charsetIndex) {
        initJoinUnits(orgNode);
        if (joinUnits.size() == 1) {
            return orgNode;
        }
        initNodeRelations(orgNode);
        JoinNode relationJoin = null;
        if (joinRelations.size() > 0) {
            //make DAG
            JoinRelationDag root = initJoinRelationDag();
            leftCartesianNodes();


            // todo:custom plan or use auto plan
            //if custom ,check plan can Follow the rules：Topological Sorting of dag, CartesianNodes

            // use auto plan
            relationJoin = makeBNFJoin(root, charsetIndex, defaultCmp);
        }
        // no relation join
        if (relationJoin == null) {
            return orgNode;
        }

        // others' node is the join units which can not optimize, just merge them
        JoinNode ret = makeJoinWithCartesianNode(relationJoin, charsetIndex);
        ret.setOrderBys(orgNode.getOrderBys());
        ret.setGroupBys(orgNode.getGroupBys());
        ret.select(orgNode.getColumnsSelected());
        ret.setLimitFrom(orgNode.getLimitFrom());
        ret.setLimitTo(orgNode.getLimitTo());
        ret.setOtherJoinOnFilter(orgNode.getOtherJoinOnFilter());
        ret.having(orgNode.getHavingFilter());
        ret.setWhereFilter(orgNode.getWhereFilter());
        ret.setAlias(orgNode.getAlias());
        ret.setWithSubQuery(orgNode.isWithSubQuery());
        ret.setContainsSubQuery(orgNode.isContainsSubQuery());
        ret.setSql(orgNode.getSql());
        ret.setUpFields();
        return ret;
    }

    private JoinNode makeJoinWithCartesianNode(JoinNode node, int charsetIndex) {
        JoinNode left = node;
        for (PlanNode right : joinUnits) {
            left = new JoinNode(left, right, charsetIndex);
        }
        return left;
    }

    private void leftCartesianNodes() {
        if (joinUnits.size() > dagNodes.size()) {
            //Cartesian Product node
            joinUnits.removeIf(dagNodes::contains);
        } else {
            joinUnits.clear();
        }
    }

    @NotNull
    private JoinRelationDag initJoinRelationDag() {
        JoinRelationDag root = createFirstNode();
        for (int i = 1; i < joinRelations.size(); i++) {
            addNodeToDag(root, joinRelations.get(i));
        }
        return root;
    }

    @NotNull
    private JoinRelationDag createFirstNode() {
        JoinRelations firstRelation = joinRelations.get(0);
        JoinRelationDag root = new JoinRelationDag(firstRelation.getLeftPlanNode());
        JoinRelationDag right = new JoinRelationDag(firstRelation);
        root.rightNodes.add(right);
        right.degree++;
        return root;
    }
    private JoinNode makeBNFJoin(JoinRelationDag root, int charsetIndex, Comparator<JoinRelationDag> joinCmp) {

        JoinNode joinNode = null;
        Queue<JoinRelationDag> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            JoinRelationDag left = queue.poll();
            List<JoinRelationDag> nextZeroDegreeList = new ArrayList<>();
            for (JoinRelationDag tree : left.rightNodes) {
                if (--tree.degree == 0) {
                    nextZeroDegreeList.add(tree);
                }
            }
            if (nextZeroDegreeList.size() > 0) {
                nextZeroDegreeList.sort(joinCmp);
                for (JoinRelationDag rightNode : nextZeroDegreeList) {
                    joinNode = makeJoinNode(charsetIndex, left, joinNode, rightNode);
                    queue.offer(rightNode);
                }
            }
        }
        return joinNode;
    }

    private JoinNode makeJoinNode(int charsetIndex, JoinRelationDag left, JoinNode joinNode, JoinRelationDag rightNodeOfJoin) {
        boolean leftIsJoin = joinNode != null;
        PlanNode leftNode = leftIsJoin ? joinNode : left.node;
        PlanNode rightNode = rightNodeOfJoin.node;
        joinNode = new JoinNode(leftNode, rightNode, charsetIndex);
        if (!rightNodeOfJoin.relations.isInner) {
            joinNode.setLeftOuterJoin();
        }
        List<ItemFuncEqual> filters = new ArrayList<>();
        for (JoinRelation joinRelation : rightNodeOfJoin.relations.relationLst) {
            ItemFuncEqual bf = FilterUtils.equal(joinRelation.left.key, joinRelation.right.key, charsetIndex);
            filters.add(bf);
            if (joinRelation.isERJoin) {
                if (!leftIsJoin) {
                    joinNode.getERkeys().add(joinRelation.left.erTable);
                } else {
                    joinNode.getERkeys().addAll(((JoinNode) leftNode).getERkeys());
                }
            }
        }
        joinNode.setJoinFilter(filters);
        joinNode.setOtherJoinOnFilter(rightNodeOfJoin.relations.otherFilter);
        return joinNode;
    }

    private void addNodeToDag(JoinRelationDag root, JoinRelations relations) {
        JoinRelationDag father = null;
        List<JoinRelationDag> otherPres = new ArrayList<>(relations.prefixNodes.size());
        boolean familyInner = relations.isInner;
        Queue<JoinRelationDag> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            JoinRelationDag tmp = queue.poll();
            List<JoinRelationDag> children = new ArrayList<>(tmp.rightNodes);
            if (relations.prefixNodes.contains(tmp.node)) {
                otherPres.add(tmp);
                --relations.prefixSize;
                if (relations.getLeftPlanNode() == tmp.node) {
                    father = tmp;
                }
                if (familyInner && !tmp.isFamilyInner) {
                    familyInner = false;
                }
            }
            //all prefixNodes finished
            if (relations.prefixSize == 0) {
                break;
            } else {
                queue.addAll(children);
            }
        }
        if (!familyInner) { // left join can not be optimizer
            JoinRelationDag right = new JoinRelationDag(relations);
            right.isFamilyInner = false;
            for (JoinRelationDag otherPre : otherPres) {
                otherPre.rightNodes.add(right);
                right.degree++;
            }
            return;
        }

        assert father != null;
        if (relations.prefixNodes.size() > 1) {
            Map<PlanNode, Item> planFilterMap = new HashMap<>();
            if (assignOtherFilter(planFilterMap, relations.otherFilter, father.node, relations.getRightPlanNode())) {
                // recreate JoinRelations, cut other filter
                JoinRelations nodeRelations = new JoinRelations(true, planFilterMap.get(father.node));
                nodeRelations.relationLst.addAll(relations.relationLst);
                nodeRelations.isERJoin = relations.isERJoin;
                JoinRelationDag right = new JoinRelationDag(nodeRelations);
                father.rightNodes.add(right);
                right.degree++;
                // update filter
                for (JoinRelationDag otherPre : otherPres) {
                    if (planFilterMap.get(otherPre.node) != null) {
                        otherPre.relations.otherFilter = FilterUtils.and(otherPre.relations.otherFilter, planFilterMap.get(otherPre.node));
                    }
                }
                return;
            }
        }
        //no other filter or can not assign
        JoinRelationDag right = new JoinRelationDag(relations);
        for (JoinRelationDag otherPre : otherPres) {
            otherPre.rightNodes.add(right);
            right.degree++;
        }
    }


    private boolean assignOtherFilter(Map<PlanNode, Item> planFilterMap, Item filter, PlanNode father, PlanNode rightNode) {
        List<Item> splitFilters = FilterUtils.splitFilter(filter);
        for (Item splitFilter : splitFilters) {
            Set<PlanNode> filterNode = new HashSet<>();
            for (PlanNode planNode : joinUnits) {
                Item tmpSel = nodeHasSelectTable(planNode, splitFilter);
                if (tmpSel != null) {
                    filterNode.add(planNode);
                    if (filterNode.size() >= 3) {
                        return false;
                    }
                }
            }
            if (filterNode.size() == 0) {
                updateMapByKey(planFilterMap, father, splitFilter);
            } else if (filterNode.size() == 1) {
                if (filterNode.contains(rightNode)) {
                    updateMapByKey(planFilterMap, father, splitFilter);
                } else {
                    updateMapByKey(planFilterMap, filterNode.iterator().next(), splitFilter);
                }
            } else { // =2
                if (filterNode.contains(father) && filterNode.contains(rightNode)) {
                    updateMapByKey(planFilterMap, father, splitFilter);
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    private void updateMapByKey(Map<PlanNode, Item> map, PlanNode key, Item value) {
        Item oldValue = map.get(key);
        map.put(key, FilterUtils.and(oldValue, value));
    }

    // find the smallest join units in node
    private void initJoinUnits(JoinNode node) {
        for (int index = 0; index < node.getChildren().size(); index++) {
            PlanNode child = node.getChildren().get(index);
            if (isUnit(child)) {
                child = JoinProcessor.optimize(child);
                node.getChildren().set(index, child);
                this.joinUnits.add(child);
            } else {
                initJoinUnits((JoinNode) child);
            }
        }
    }


    private boolean isUnit(PlanNode node) {
        return node.type() != PlanNode.PlanNodeType.JOIN || node.isWithSubQuery();
    }

    private void initNodeRelations(JoinNode joinNode) {
        for (PlanNode unit : joinUnits) {
            // is unit
            if (unit == joinNode) {
                return;
            }
        }
        for (PlanNode child : joinNode.getChildren()) {
            if ((!isUnit(child)) && (child.type().equals(PlanNode.PlanNodeType.JOIN))) {
                initNodeRelations((JoinNode) child);
            }
        }

        if (joinNode.getJoinFilter().size() > 0) {
            JoinRelations nodeRelations = new JoinRelations(joinNode.isInnerJoin(), joinNode.getOtherJoinOnFilter());
            for (ItemFuncEqual filter : joinNode.getJoinFilter()) {
                JoinColumnInfo columnInfoLeft = initJoinColumnInfo(filter.arguments().get(0));
                JoinColumnInfo columnInfoRight = initJoinColumnInfo(filter.arguments().get(1));
                JoinRelation nodeRelation = new JoinRelation(columnInfoLeft, columnInfoRight);
                nodeRelations.relationLst.add(nodeRelation);
                if (nodeRelation.isERJoin) {
                    nodeRelations.isERJoin = true;
                }
            }
            nodeRelations.init();
            joinRelations.add(nodeRelations);
        }
    }

    private JoinColumnInfo initJoinColumnInfo(Item key) {
        JoinColumnInfo columnInfoLeft = new JoinColumnInfo(key);
        for (PlanNode planNode : joinUnits) {
            Item tmpSel = nodeHasSelectTable(planNode, columnInfoLeft.key);
            if (tmpSel != null) {
                columnInfoLeft.planNode = planNode;
                columnInfoLeft.erTable = getERKey(planNode, tmpSel);
                return columnInfoLeft;
            }
        }
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can not find table of:" + key);
    }


    private ERTable getERKey(PlanNode tn, Item c) {
        if (!(c instanceof ItemField))
            return null;
        if (tn.type() != PlanNode.PlanNodeType.TABLE && !PlanUtil.isERNode(tn)) {
            return null;
        }
        Pair<TableNode, ItemField> pair = PlanUtil.findColumnInTableLeaf((ItemField) c, tn);
        if (pair == null)
            return null;
        TableNode tableNode = pair.getKey();
        ItemField col = pair.getValue();
        ERTable erTable = new ERTable(tableNode.getSchema(), tableNode.getPureName(), col.getItemName());
        if (tn.type() == PlanNode.PlanNodeType.TABLE) {
            return erTable;
        } else {
            List<ERTable> erList = ((JoinNode) tn).getERkeys();
            for (ERTable cerKey : erList) {
                if (isErRelation(erTable, cerKey))
                    return erTable;
            }
            return null;
        }
    }

    //TODO:performance

    private boolean isGlobalTree(PlanNode tn) {
        if (tn instanceof TableNode && tn.getSubQueries().size() == 0) {
            return tn.getUnGlobalTableCount() == 0;
        } else if (tn.type() == PlanNode.PlanNodeType.NONAME) {
            return tn.getSubQueries().size() == 0;
        } else {
            for (TableNode leaf : tn.getReferedTableNodes()) {
                if (leaf.getUnGlobalTableCount() != 0)
                    return false;
            }
            return true;
        }
    }

    private Item nodeHasSelectTable(PlanNode child, Item sel) {
        if (sel instanceof ItemField) {
            return nodeHasColumn(child, (ItemField) sel);
        } else if (sel.canValued()) {
            return sel;
        } else if (sel.type().equals(Item.ItemType.SUM_FUNC_ITEM)) {
            return null;
        } else {
            ItemFunc fcopy = (ItemFunc) sel.cloneStruct();
            for (int index = 0; index < fcopy.getArgCount(); index++) {
                Item arg = fcopy.arguments().get(index);
                Item argSel = nodeHasSelectTable(child, arg);
                if (argSel == null)
                    return null;
                else
                    fcopy.arguments().set(index, argSel);
            }
            PlanUtil.refreshReferTables(fcopy);
            fcopy.setPushDownName(null);
            return fcopy;
        }
    }

    private Item nodeHasColumn(PlanNode child, ItemField col) {
        String colName = col.getItemName();
        if (StringUtil.isEmpty(col.getTableName())) {
            for (Entry<NamedField, Item> entry : child.getOuterFields().entrySet()) {
                if (StringUtil.equalsIgnoreCase(colName, entry.getKey().getName())) {
                    return entry.getValue();
                }
            }
            return null;
        } else {
            String table = col.getTableName();
            if (child.getAlias() == null) {
                for (Entry<NamedField, Item> entry : child.getOuterFields().entrySet()) {
                    if (StringUtil.equals(table, entry.getKey().getTable()) &&
                            StringUtil.equalsIgnoreCase(colName, entry.getKey().getName())) {
                        return entry.getValue();
                    }
                }
            } else {
                if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                    if (!StringUtil.equalsIgnoreCase(table, child.getAlias()))
                        return null;
                } else {
                    if (!StringUtil.equals(table, child.getAlias()))
                        return null;
                }
                for (Entry<NamedField, Item> entry : child.getOuterFields().entrySet()) {
                    if (StringUtil.equalsIgnoreCase(colName, entry.getKey().getName())) {
                        return entry.getValue();
                    }
                }
            }
            return null;
        }
    }

    private boolean isErRelation(ERTable er0, ERTable er1) {
        if (er0 == null || er1 == null) {
            return false;
        }
        Set<ERTable> erList = erRelations.get(er0);
        if (erList == null) {
            return false;
        }
        return erList.contains(er1);
    }

    private class JoinRelationDag {
        private int degree = 0;
        private final PlanNode node;
        private final JoinRelations relations;
        private final List<JoinRelationDag> rightNodes = new ArrayList<>();
        private boolean isFamilyInner = true;

        JoinRelationDag(PlanNode node) {
            this.node = node;
            this.relations = null;
            dagNodes.add(node);
        }

        JoinRelationDag(JoinRelations relations) {
            this.node = relations.getRightPlanNode();
            this.relations = relations;
            this.isFamilyInner = relations.isInner;
            dagNodes.add(node);
        }
    }

    private class JoinRelations {
        private final List<JoinRelation> relationLst = new ArrayList<>();
        private final boolean isInner;
        private Item otherFilter;
        private boolean isERJoin = false;
        private final Set<PlanNode> prefixNodes = new HashSet<>();
        private int prefixSize = 0;

        JoinRelations(boolean isInner, Item otherFilter) {
            this.isInner = isInner;
            this.otherFilter = otherFilter;
        }

        PlanNode getLeftPlanNode() {
            return relationLst.get(0).left.planNode;
        }

        PlanNode getRightPlanNode() {
            return relationLst.get(0).right.planNode;
        }

        void init() {
            prefixNodes.add(getLeftPlanNode());
            if (otherFilter != null && otherFilter.getReferTables() != null) {
                for (PlanNode planNode : joinUnits) {
                    if (planNode != getRightPlanNode()) {
                        Item tmpSel = nodeHasSelectTable(planNode, otherFilter);
                        if (tmpSel != null) {
                            prefixNodes.add(planNode);
                        }
                    }
                }
            }
            prefixSize = prefixNodes.size();
        }
    }

    private class JoinRelation {
        private final JoinColumnInfo left;
        private final JoinColumnInfo right;
        private final boolean isERJoin;

        JoinRelation(JoinColumnInfo left, JoinColumnInfo right) {
            this.left = left;
            this.right = right;
            this.isERJoin = isErRelation(left.erTable, right.erTable);
        }
    }

    /**
     * JoinColumnInfo
     *
     * @author ActionTech
     */
    private static class JoinColumnInfo {
        private Item key; // join on's on key
        private PlanNode planNode; // treenode of the joinColumn belong to
        private ERTable erTable; //  joinColumn is er ,if so,save th parentkey

        JoinColumnInfo(Item key) {
            this.key = key;
            planNode = null;
            erTable = null;
        }

        @Override
        public int hashCode() {
            int hash = this.key.getTableName().hashCode();
            hash = hash * 31 + this.key.getItemName().toLowerCase().hashCode();
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null)
                return false;
            if (o == this)
                return true;
            if (!(o instanceof JoinColumnInfo)) {
                return false;
            }
            JoinColumnInfo other = (JoinColumnInfo) o;
            if (this.key == null)
                return false;
            return StringUtil.equals(this.key.getTableName(), other.key.getTableName()) &&
                    StringUtil.equalsIgnoreCase(this.key.getItemName(), other.key.getItemName());
        }

        @Override
        public String toString() {
            return "key:" + key;
        }

        public Item getKey() {
            return key;
        }

        public void setKey(Item key) {
            this.key = key;
        }

        public PlanNode getPlanNode() {
            return planNode;
        }

        public void setPlanNode(PlanNode planNode) {
            this.planNode = planNode;
        }

        public ERTable getErTable() {
            return erTable;
        }

        public void setErTable(ERTable erTable) {
            this.erTable = erTable;
        }
    }
}
