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

import java.util.*;
import java.util.Map.Entry;

public class JoinChooser {

    private final List<JoinRelations> joinRelations = new ArrayList<>();
    private final List<PlanNode> joinUnits = new ArrayList<>();
    private final Map<ERTable, Set<ERTable>> erRelations;
    private final JoinNode orgNode;
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
     *
     */
    private JoinNode innerJoinOptimizer(int charsetIndex) {
        initJoinUnits(orgNode);
        if (joinUnits.size() == 1) {
            return orgNode;
        }
        initNodeRelations(orgNode);
        JoinNode relationJoin = null;
        if (joinRelations.size() > 0) {
            //todo: remove from joinUnits
            JoinRelationTree root = new JoinRelationTree(joinRelations.get(0).getLeftPlanNode(), null);
            initRightOfTree(root, joinRelations.get(0));
            for (int i = 1; i < joinRelations.size(); i++) {
                findTreeLeftAndInitRight(root, joinRelations.get(i));
            }
            //todo :other joinUnits means 笛卡尔积？

            // todo custom plan or auto plan
            //if no custom
            relationJoin = makeBNFJoin(root, charsetIndex);

        }

        if (relationJoin == null)
            // no er join
            return orgNode;

        // others' node is the join units which can not optimize, just merge them
        JoinNode ret = relationJoin;
        ret.setOrderBys(orgNode.getOrderBys());
        ret.setGroupBys(orgNode.getGroupBys());
        ret.select(orgNode.getColumnsSelected());
        ret.setLimitFrom(orgNode.getLimitFrom());
        ret.setLimitTo(orgNode.getLimitTo());
        ret.having(orgNode.getHavingFilter());
        ret.setWhereFilter(orgNode.getWhereFilter());
        ret.setAlias(orgNode.getAlias());
        ret.setWithSubQuery(orgNode.isWithSubQuery());
        ret.setContainsSubQuery(orgNode.isContainsSubQuery());
        ret.setSql(orgNode.getSql());
        ret.setUpFields();
        return ret;
    }

    private JoinNode makeBNFJoin(JoinRelationTree root, int charsetIndex) {
        Comparator<JoinRelationTree> joinCmp = (o1, o2) -> {
            // if o1 is ER join ,we keep order o1<o2
            if (o1.relations.isERJoin) {
                return -1;
            }
            // if o1 is not ER join, o2 is ER join, o1>o2
            if (o2.relations.isERJoin) {
                return 1;
            }

            // both o1,o2 are not ER join, global table should be first
            // todo: both global table
            // if o1 is global table ,we keep order o1<o2
            if (o1.node.getUnGlobalTableCount() == 0) {
                return -1;
            }
            // if o1 is not global table, o2 is global table, o1>o2
            if (o2.node.getUnGlobalTableCount() == 0) {
                return 1;
            }
            return 0;
        };

        JoinNode joinNode = null;
        Queue<JoinRelationTree> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            JoinRelationTree left = queue.poll();
            left.rightNodesOfInnerJoin.sort(joinCmp);
            for (JoinRelationTree rightNodeOfInnerJoin : left.rightNodesOfInnerJoin) {
                joinNode = makeJoinNode(charsetIndex, left, joinNode, rightNodeOfInnerJoin);
                queue.offer(rightNodeOfInnerJoin);
            }
            left.rightNodesOfLeftJoin.sort(joinCmp);
            for (JoinRelationTree rightNodeOfLeftJoin : left.rightNodesOfLeftJoin) {
                joinNode = makeJoinNode(charsetIndex, left, joinNode, rightNodeOfLeftJoin);
                joinNode.setLeftOuterJoin();
                queue.offer(rightNodeOfLeftJoin);
            }
        }
        return joinNode;
    }

    private JoinNode makeJoinNode(int charsetIndex, JoinRelationTree left, JoinNode joinNode, JoinRelationTree rightNodeOfJoin) {
        PlanNode leftNode = joinNode == null ? left.node : joinNode;
        PlanNode rightNode = rightNodeOfJoin.node;
        joinNode = new JoinNode(leftNode, rightNode, charsetIndex);
        List<ItemFuncEqual> filters = new ArrayList<>();
        for (JoinRelation joinRelation : rightNodeOfJoin.relations.relationLst) {
            ItemFuncEqual bf = FilterUtils.equal(joinRelation.left.key, joinRelation.right.key, charsetIndex);
            filters.add(bf);
            if (joinRelation.isERJoin) {
                joinNode.getERkeys().add(joinRelation.left.erTable);
            }
        }
        joinNode.setJoinFilter(filters);
        joinNode.setOtherJoinOnFilter(rightNodeOfJoin.relations.otherFilter);
        return joinNode;
    }

    private void findTreeLeftAndInitRight(JoinRelationTree root, JoinRelations relations) {
        Queue<JoinRelationTree> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            JoinRelationTree tmp = queue.poll();
            if (tmp.node == relations.getLeftPlanNode()) {
                initRightOfTree(tmp, relations);
                break;
            } else {
                queue.addAll(tmp.rightNodesOfInnerJoin);
                queue.addAll(tmp.rightNodesOfLeftJoin);
            }
        }
    }

    private void initRightOfTree(JoinRelationTree root, JoinRelations relations) {
        JoinRelationTree right = new JoinRelationTree(relations.getRightPlanNode(), relations);
        if (relations.isInner) {
            root.rightNodesOfInnerJoin.add(right);
        } else {
            root.rightNodesOfLeftJoin.add(right);
        }
    }


    // find the smallest join units in node
    private void initJoinUnits(JoinNode node) {
        for (int index = 0; index < node.getChildren().size(); index++) {
            PlanNode child = node.getChildren().get(index);
            if (isUnit(child)) {
                child = JoinERProcessor.optimize(child); //todo:change class name
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





    //todo:remove
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
                return null;
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
                return null;
            }
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

    private static class JoinRelationTree {
        private final PlanNode node;
        private final JoinRelations relations;
        private final List<JoinRelationTree> rightNodesOfLeftJoin = new ArrayList<>();
        private final List<JoinRelationTree> rightNodesOfInnerJoin = new ArrayList<>();

        JoinRelationTree(PlanNode node, JoinRelations relations) {
            this.node = node;
            this.relations = relations;
        }
    }
    private class JoinRelations {
        private final List<JoinRelation> relationLst = new ArrayList<>();
        private final boolean isInner;
        private final Item otherFilter;
        private boolean isERJoin = false;

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
