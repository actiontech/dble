/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.model.sharding.table.ERTable;
import com.oceanbase.obsharding_d.plan.NamedField;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemField;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.oceanbase.obsharding_d.plan.node.JoinNode;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.plan.util.FilterUtils;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.util.*;
import java.util.Map.Entry;

public class ERJoinChooser {
    /**
     * record all join relation in join node ex: t1 inner join t2 on t1.id=t2.id inner join t3 on t1.name=t3.name and t1.id=t3.id
     * then:
     * joinRelationGroups[0]: t1.id=t2.id, t1.id=t3.id
     * joinRelationGroups[1]: t1.name=t3.name
     */
    private ArrayList<JoinRelationGroup> joinRelationGroups = new ArrayList<>();

    private final List<PlanNode> joinUnits = new ArrayList<>();

    // global table
    private final List<PlanNode> globals = new ArrayList<>();

    // make er joinnode
    private final List<JoinNode> makedERJnList = new ArrayList<>();

    private final List<Item> otherJoinOns = new ArrayList<>();

    private JoinNode orgNode = null;

    private final Map<ERTable, Set<ERTable>> erRelations;

    public ERJoinChooser(JoinNode qtn, Map<ERTable, Set<ERTable>> erRelations) {
        this.orgNode = qtn;
        this.erRelations = erRelations;
    }

    public ERJoinChooser(JoinNode qtn) {
        this(qtn, OBsharding_DServer.getInstance().getConfig().getErRelations());
    }

    public JoinNode optimize() {
        if (erRelations == null) {
            return orgNode;
        }
        if (orgNode.isLeftOuterJoin()) {
            if (orgNode.isNotIn()) {
                return orgNode;
            }
            return leftJoinOptimizer();
        } else { // (jn.isInnerJoin()) {
            return innerJoinOptimizer(orgNode.getCharsetIndex());
        }
    }

    /* ------------------- left join optimizer start -------------------- */

    /**
     * left join's ER is different from inner join's
     * ex:t1,t2 ,if t1 left join t2 on
     * t1.id=t2.id can be pushed
     * < we can't change left join's structure>
     */
    private JoinNode leftJoinOptimizer() {
        PlanNode left = orgNode.getLeftNode();
        PlanNode right = orgNode.getRightNode();
        if (left.type() == PlanNode.PlanNodeType.JOIN) {
            left = JoinERProcessor.optimize(left);
            orgNode.setLeftNode(left);
        }
        if (right.type() == PlanNode.PlanNodeType.JOIN) {
            right = JoinERProcessor.optimize(right);
            orgNode.setRightNode(right);
        }
        for (ItemFuncEqual filter : orgNode.getJoinFilter()) {
            ERTable leftER = getLeftOutJoinChildER(orgNode, left, filter.arguments().get(0));
            ERTable rightER = getLeftOutJoinChildER(orgNode, right, filter.arguments().get(1));
            if (isErRelation(leftER, rightER)) {
                orgNode.getERkeys().add(leftER);
            }
        }
        return orgNode;
    }

    private ERTable getLeftOutJoinChildER(JoinNode joinNode, PlanNode child, Item onItem) {
        if (PlanUtil.existAggregate(child))
            return null;
        else if (!PlanUtil.isERNode(child) && child.type() != PlanNode.PlanNodeType.TABLE)
            return null;

        if (onItem == null || !onItem.type().equals(Item.ItemType.FIELD_ITEM))
            return null;
        Pair<TableNode, ItemField> joinColumnInfo = PlanUtil.findColumnInTableLeaf((ItemField) onItem, joinNode);
        if (joinColumnInfo == null)
            return null;
        TableNode tn = joinColumnInfo.getKey();
        ItemField col = joinColumnInfo.getValue();
        ERTable erKey = new ERTable(tn.getSchema(), tn.getPureName(), col.getItemName());
        if (child.type() == PlanNode.PlanNodeType.TABLE) {
            return erKey;
        } else { // joinnode
            List<ERTable> erKeys = ((JoinNode) child).getERkeys();
            for (ERTable cerKey : erKeys) {
                if (isErRelation(erKey, cerKey))
                    return erKey;
            }
            return null;
        }
    }


    /* ------------------- left join optimizer end -------------------- */

    /**
     * inner join's ER, rebuild inner join's unit
     */
    private JoinNode innerJoinOptimizer(int charsetIndex) {
        initInnerJoinUnits(orgNode);
        if (joinUnits.size() == 1) {
            return orgNode;
        }
        visitJoinOns(orgNode);
        for (JoinRelationGroup group : joinRelationGroups) {
            JoinNode erJoinNode = tryMakeERJoin(group.getSelList(), charsetIndex);
            if (erJoinNode != null) {
                this.makedERJnList.add(erJoinNode);
            }
        }
        adjustJoinRelationGroups();

        if (makedERJnList.isEmpty())
            // no er join
            return orgNode;

        List<PlanNode> others = new ArrayList<>();
        // make makedErJnList at the beginning,join with ER
        others.addAll(makedERJnList);
        others.addAll(joinUnits);
        for (int i = 0; i < others.size(); i++) {
            // make up the unit which cna;t optimized  and global table
            PlanNode tnewOther = others.get(i);
            PlanNode newT0 = joinWithGlobal(tnewOther, globals, charsetIndex);
            others.set(i, newT0);
        }
        // only others and globals may have node and have been tried to ER JOIN
        if (globals.size() > 0) {
            PlanNode globalJoin = makeJoinNode(globals, charsetIndex);
            others.add(globalJoin);
        }
        // others' node is the join units which can not optimize, just merge them
        JoinNode ret = (JoinNode) makeJoinNode(others, charsetIndex);
        ret.setDistinct(orgNode.isDistinct());
        ret.setOrderBys(orgNode.getOrderBys());
        ret.setGroupBys(orgNode.getGroupBys());
        ret.select(orgNode.getColumnsSelected());
        ret.setLimitFrom(orgNode.getLimitFrom());
        ret.setLimitTo(orgNode.getLimitTo());
        ret.setOtherJoinOnFilter(FilterUtils.and(orgNode.getOtherJoinOnFilter(), FilterUtils.and(otherJoinOns)));
        Item unFoundSelFilter = makeRestFilter();
        if (unFoundSelFilter != null)
            ret.setOtherJoinOnFilter(FilterUtils.and(ret.getOtherJoinOnFilter(), unFoundSelFilter));
        // and the origin where and the remain condition in selLists
        ret.having(orgNode.getHavingFilter());
        ret.setWhereFilter(orgNode.getWhereFilter());
        ret.setAlias(orgNode.getAlias());
        ret.setWithSubQuery(orgNode.isWithSubQuery());
        ret.setContainsSubQuery(orgNode.isContainsSubQuery());
        ret.getSubQueries().addAll(orgNode.getSubQueries());
        ret.setSql(orgNode.getSql());
        ret.setUpFields();
        return ret;
    }

    /**
     * in an JoinRelationGroup, take the er related JoinColumnInfos, to start makeERJoin
     */
    private JoinNode tryMakeERJoin(List<JoinColumnInfo> selList, int charsetIndex) {
        if (joinUnits.size() <= 1) {
            return null;
        }
        List<JoinColumnInfo> erKeys = new ArrayList<>();
        for (int i = 0; i < selList.size(); i++) {
            JoinColumnInfo jki = selList.get(i);
            if (jki.erTable == null)
                continue;
            for (int j = i + 1; j < selList.size(); j++) {
                JoinColumnInfo jkj = selList.get(j);
                if (isErRelation(jki.erTable, jkj.erTable)) {
                    erKeys.add(jkj);
                }
            }
            if (!erKeys.isEmpty()) {
                // er found
                erKeys.add(0, jki);
                return makeERJoin(erKeys, charsetIndex);
            }
        }
        return null;
    }

    private JoinNode makeERJoin(List<JoinColumnInfo> erKeys, int charsetIndex) {
        PlanNode t0 = erKeys.get(0).planNode;
        PlanNode t1;
        for (int index = 1; index < erKeys.size(); index++) {
            t1 = erKeys.get(index).planNode;
            if (t1 instanceof JoinNode) {
                if (t1 == t0) {
                    /**
                     * ex: a left b inner c on a.id=c.id and b.id=c.id, and er relations in the a.id and b.id, c.id
                     * where a.id'PlanNode == b.id'PlanNode, so ignore
                     */
                    joinUnits.remove(t1);
                }
                // else, it's not what expected
                continue;
            } else {
                t0 = toMakeERJoin(t0, t1, charsetIndex, true);
            }
        }
        return (JoinNode) t0;
    }

    private JoinNode toMakeERJoin(PlanNode t0, PlanNode t1, int charsetIndex, boolean isReplace) {
        JoinNode newJoinNode = new JoinNode(t0, t1, charsetIndex);
        List<ItemFuncEqual> joinFilter = toMakeJoinFilters(newJoinNode, t0, t1, charsetIndex, true);
        newJoinNode.setJoinFilter(joinFilter);
        return newJoinNode;
    }

    /**
     * just makeJoinNode according with wishes
     *
     * @param units
     * @return
     */
    private PlanNode makeJoinNode(List<PlanNode> units, int charsetIndex) {
        PlanNode ret = units.get(0);
        for (int i = 1; i < units.size(); i++) {
            ret = toMakeERJoin(ret, units.get(i), charsetIndex, true);
        }
        return ret;
    }

    /**
     * join t0 and t1, find er relation in joinRelationGroups and make joinFilter.
     * <p>
     * 1.directly find relation, directly just push
     * such as: 'a join b join c on a.id=c.id', a.id and c.id is er relations
     * <p>
     * 2.indirect find relation, make new joinFilter then push
     * such as: 'a join b join c on a.id=c.id and b.id=c.id ', Only a.id and b.id are related, so we will make 'a.id = b.id' and push it.
     */
    private List<ItemFuncEqual> toMakeJoinFilters(JoinNode newJoinNode, PlanNode t0, PlanNode t1, int charsetIndex, boolean isReplace) {
        List<ItemFuncEqual> filters = new ArrayList<>();
        for (JoinRelationGroup group : joinRelationGroups) {
            ArrayList<JoinRelation> tempOnList = new ArrayList<>(group.getRelationList());
            // 1.directly find relation
            for (JoinRelation on : tempOnList) {
                boolean isHave = false;
                if (on.getLeft().getPlanNode() == t0 && on.getRight().getPlanNode() == t1) {
                    isHave = true;
                } else if (on.getLeft().getPlanNode() == t1 && on.getRight().getPlanNode() == t0) {
                    isHave = true;
                }

                if (isHave) {
                    if (isErRelation(on.getLeft().getErTable(), on.getRight().getErTable())) {
                        filters.add(on.getFilter());
                        group.getRelationList().remove(on);
                        newJoinNode.getERkeys().add(on.getLeft().getErTable());
                    }
                }
            }

            // 2.indirect find relation
            if (filters.isEmpty() && !tempOnList.isEmpty()) {
                ArrayList<JoinColumnInfo> cols = new ArrayList<>(group.getSelList());
                JoinColumnInfo col0 = null;
                JoinColumnInfo col1 = null;
                for (JoinColumnInfo col : cols) {
                    if (col.planNode == t0) {
                        col0 = col;
                    } else if (col.planNode == t1) {
                        col1 = col;
                    }
                }
                if (col0 != null && col1 != null && isErRelation(col0.getErTable(), col1.getErTable())) {
                    ItemFuncEqual bf = FilterUtils.equal(col0.key, col1.key, charsetIndex);
                    filters.add(bf);
                    newJoinNode.getERkeys().add(col0.getErTable());
                }
            }
        }

        // t0 and t1 involved, so joinUnits will remove them
        joinUnits.remove(t0);
        joinUnits.remove(t1);

        if (isReplace)
            replaceReferedInJoinRelation(newJoinNode, t0, t1);

        return filters;
    }

    private PlanNode joinWithGlobal(PlanNode t, List<PlanNode> globalList, int charsetIndex) {
        PlanNode newT = t;
        while (globalList.size() > 0) {
            boolean foundJoin = false;
            for (int i = 0; i < globalList.size(); i++) {
                PlanNode global = globalList.get(i);
                // try join
                JoinNode joinNode = new JoinNode(newT, global, charsetIndex);
                List<ItemFuncEqual> jnFilter = toMakeJoinFilters(joinNode, newT, global, charsetIndex, false);
                // @if no join column, then the other is cross join
                if (jnFilter.size() > 0 || joinRelationGroups.size() == 0) {
                    replaceReferedInJoinRelation(joinNode, newT, global);
                    foundJoin = true;
                    joinNode.setJoinFilter(jnFilter);
                    globalList.remove(i);
                    newT = joinNode;
                    break;
                }
            }
            if (!foundJoin) // no join can do from t and globals
                break;
        }
        return newT;
    }

    // replace referenced planNode in joinRelation
    private void replaceReferedInJoinRelation(JoinNode newJoinNode, PlanNode t0, PlanNode t1) {
        for (JoinRelationGroup group : joinRelationGroups) {
            for (JoinRelation ri : group.getRelationList()) {
                if (ri.getLeft().planNode == t0 || ri.getLeft().planNode == t1) {
                    ri.getLeft().planNode = newJoinNode;
                }
                if (ri.getRight().planNode == t0 || ri.getRight().planNode == t1) {
                    ri.getRight().planNode = newJoinNode;
                }
            }
        }
    }

    /**
     * JoinColumnInfo: add joinUnit、erTable
     */
    public JoinColumnInfo initJoinColumnInfo(Item key) {
        JoinColumnInfo columnInfo = new JoinColumnInfo(key);
        for (PlanNode planNode : joinUnits) {
            Item tmpSel = nodeHasSelectTable(planNode, columnInfo.key);
            if (tmpSel != null) {
                columnInfo.planNode = planNode;
                columnInfo.erTable = getERKey(planNode, tmpSel);
                return columnInfo;
            }
        }
        return columnInfo;
    }

    /**
     * find the smallest join units in node
     *
     * @param node innerjoin
     */
    private void initInnerJoinUnits(JoinNode node) {
        initJoinUnits(node, true);
    }

    private void initJoinUnits(JoinNode node, boolean isInner) {
        if (isGlobalTree(node)) {
            this.globals.add(node);
        } else {
            for (int index = 0; index < node.getChildren().size(); index++) {
                PlanNode child = node.getChildren().get(index);
                if (isUnit(child, isInner)) {
                    child = JoinERProcessor.optimize(child);
                    node.getChildren().set(index, child);
                    this.joinUnits.add(child);
                } else {
                    initInnerJoinUnits((JoinNode) child);
                }
            }
        }
    }

    private boolean isUnit(PlanNode node, boolean isInner) {
        if (isGlobalTree(node))
            return true;
        else
            return node.type() != PlanNode.PlanNodeType.JOIN || node.isWithSubQuery() || !(isInner && ((JoinNode) node).isInnerJoin());
    }

    /**
     * visitJoinOns
     *
     * @param joinNode joinNode which to visit
     */
    private void visitJoinOns(JoinNode joinNode) {
        for (PlanNode unit : joinUnits) {
            // is unit
            if (unit == joinNode) {
                return;
            }
        }

        for (ItemFuncEqual filter : joinNode.getJoinFilter()) {
            addJoinRelation(filter);
        }

        for (PlanNode child : joinNode.getChildren()) {
            if ((!isUnit(child, true)) && (child.type().equals(PlanNode.PlanNodeType.JOIN))) {
                // a join b on a.id=b.id and a.id+b.id=10 join c on
                // a.id=c.id,push up a.id+b.id
                JoinNode jnChild = (JoinNode) child;
                if (jnChild.getOtherJoinOnFilter() != null)
                    otherJoinOns.add(jnChild.getOtherJoinOnFilter());

                visitJoinOns((JoinNode) child);
            }
        }
    }

    /**
     * parser joinFilter into joinRelationGroups
     *
     * @param filter
     */
    private void addJoinRelation(ItemFuncEqual filter) {
        JoinColumnInfo jiLeft = initJoinColumnInfo(filter.arguments().get(0));
        JoinColumnInfo jiRight = initJoinColumnInfo(filter.arguments().get(1));

        JoinRelation relation = new JoinRelation(jiLeft, jiRight, filter);
        for (JoinRelationGroup group : joinRelationGroups) {
            List<JoinColumnInfo> onColumns = group.getSelList();

            if (onColumns.contains(jiLeft)) {
                toMoveRelation(group, jiRight, relation);
                return;
            } else if (onColumns.contains(jiRight)) {
                toMoveRelation(group, jiLeft, relation);
                return;
            }
        }

        JoinRelationGroup group = new JoinRelationGroup();
        group.addNew(relation);
        joinRelationGroups.add(group);

        adjustJoinRelationGroups();
    }

    public void adjustJoinRelationGroups() {
        List<JoinRelationGroup> groups = new ArrayList<>(joinRelationGroups);
        for (JoinRelationGroup g : groups) {
            if (g.getRelationList().size() == 0) {
                joinRelationGroups.remove(g);
            }
        }
    }

    /**
     * eg:
     * JoinRelationGroup[0] a.id=b.id, b.id=c.id2
     * JoinRelationGroup[1] d.id=e.id, f.id=j.id2
     * <p>
     * now there is a joinFilter is d.id=c.id2
     * we must put c.id2 into JoinRelationGroup[0], add pull JoinRelationGroup[1]'s item into JoinRelationGroup[0]
     * <p>
     * final, JoinRelationGroup[0] a.id=b.id, b.id=c.id2, d.id=e.id, f.id=j.id2, d.id=c.id2
     */
    private void toMoveRelation(JoinRelationGroup currGroup, JoinColumnInfo currColumn, JoinRelation relation) {
        List<JoinRelationGroup> groups = new ArrayList<>(joinRelationGroups);
        for (JoinRelationGroup group : groups) {
            if (currGroup == group)
                continue;

            if (group.getSelList().contains(currColumn)) {
                currGroup.addAll(group);
            }
        }
        currGroup.addNew(relation);
    }

    /**
     * the origin filter is join on filter,
     * now join is changed, and the filter is not joinFilter any more ,add it to otherJoinFilter
     */
    private Item makeRestFilter() {
        Item filter = null;
        for (JoinRelationGroup group : joinRelationGroups) {
            for (JoinRelation r : group.getRelationList()) {
                ItemFuncEqual bf = r.getFilter();
                filter = FilterUtils.and(filter, bf);
            }
        }
        return filter;
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
                return null;
            } else {
                if (OBsharding_DServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
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

    /**
     * JoinColumnInfo
     *
     * @author oceanbase
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

    private static class JoinRelation {
        private final JoinColumnInfo left;
        private final JoinColumnInfo right;
        private final ItemFuncEqual filter;

        JoinRelation(JoinColumnInfo left, JoinColumnInfo right, ItemFuncEqual filter) {
            this.left = left;
            this.right = right;
            this.filter = filter;
        }

        public JoinColumnInfo getLeft() {
            return left;
        }

        public JoinColumnInfo getRight() {
            return right;
        }

        public ItemFuncEqual getFilter() {
            return filter;
        }
    }

    private static class JoinRelationGroup {
        private List<JoinRelation> relationList;

        JoinRelationGroup() {
            this.relationList = new ArrayList<>();
        }

        public void addNew(JoinRelation relation) {
            relationList.add(relation);
        }

        public void addAll(JoinRelationGroup oldGroup) {
            relationList.addAll(oldGroup.getRelationList());
            oldGroup.clear();
        }

        public List<JoinRelation> getRelationList() {
            return relationList;
        }

        public List<JoinColumnInfo> getSelList() {
            List<JoinColumnInfo> selList0 = new ArrayList<>();
            if (relationList.size() == 0)
                return selList0;

            relationList.forEach(relation -> {
                if (!selList0.contains(relation.getLeft())) {
                    selList0.add(relation.getLeft());
                }
                if (!selList0.contains(relation.getRight())) {
                    selList0.add(relation.getRight());
                }
            });
            return selList0;
        }

        public void clear() {
            relationList.clear();
        }
    }
}
