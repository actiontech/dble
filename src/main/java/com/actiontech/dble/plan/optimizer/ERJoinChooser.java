/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.sharding.table.ERTable;
import com.actiontech.dble.plan.NamedField;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ERJoinChooser {
    /**
     * record all join relation in join node ex: t1 inner join t2 on t1.id=t2.id inner join t3 on
     * t1.name=t3.name and t1.id=t3.id then sel[0]: t1.id,t2.id,t3.id sel[1]:
     * t1.name,t3.name
     */
    private List<ArrayList<JoinColumnInfo>> selLists = new ArrayList<>();

    // the index  selLists for trying ER
    private int trySelListIndex = 0;

    private List<PlanNode> joinUnits = new ArrayList<>();

    // global table
    private List<PlanNode> globals = new ArrayList<>();

    // make er joinnode
    private List<JoinNode> makedERJnList = new ArrayList<>();

    private List<Item> otherJoinOns = new ArrayList<>();

    private JoinNode jn = null;

    private Map<ERTable, Set<ERTable>> erRelations;

    public ERJoinChooser(JoinNode qtn, Map<ERTable, Set<ERTable>> erRelations) {
        this.jn = qtn;
        this.erRelations = erRelations;
    }

    public ERJoinChooser(JoinNode qtn) {
        this(qtn, DbleServer.getInstance().getConfig().getErRelations());
    }

    public JoinNode optimize() {
        if (erRelations == null) {
            return jn;
        }
        if (jn.isLeftOuterJoin() && !jn.isNotIn()) {
            return leftJoinOptimizer();
        } else { // (jn.isInnerJoin()) {
            return innerJoinOptimizer(jn.getCharsetIndex());
        }
    }

    /* ------------------- left join optimizer start -------------------- */

    /**
     * left join's ER is different from inner join's
     * ex:t1,t2 ,if t1 left join t2 on
     * t1.id=t2.id can be pushed
     * < we cna't change left join's structure>
     *
     * @return
     */
    private JoinNode leftJoinOptimizer() {
        PlanNode left = jn.getLeftNode();
        PlanNode right = jn.getRightNode();
        if (left.type() == PlanNode.PlanNodeType.JOIN) {
            left = JoinERProcessor.optimize(left);
            jn.setLeftNode(left);
        }
        if (right.type() == PlanNode.PlanNodeType.JOIN) {
            right = JoinERProcessor.optimize(right);
            jn.setRightNode(right);
        }
        for (ItemFuncEqual filter : jn.getJoinFilter()) {
            ERTable leftER = getLeftOutJoinChildER(jn, left, filter.arguments().get(0));
            ERTable rightER = getLeftOutJoinChildER(jn, right, filter.arguments().get(1));
            if (isErRelation(leftER, rightER)) {
                jn.getERkeys().add(leftER);
            }
        }
        return jn;
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
     * inner join's ER, rebuild inner joi's unit
     *
     * @return
     */
    private JoinNode innerJoinOptimizer(int charsetIndex) {
        initInnerJoinUnits(jn);
        if (joinUnits.size() == 1) {
            return jn;
        }
        visitJoinOns(jn);
        initJoinColumnInfo();
        while (trySelListIndex < selLists.size()) {
            List<JoinColumnInfo> selList = selLists.get(trySelListIndex);
            JoinNode erJoinNode = tryMakeERJoin(selList, charsetIndex);
            if (erJoinNode == null) {
                trySelListIndex++;
            } else {
                // re scanning
                this.makedERJnList.add(erJoinNode);
            }
        }
        if (makedERJnList.isEmpty())
            // no er join
            return jn;

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
        ret.setOrderBys(jn.getOrderBys());
        ret.setGroupBys(jn.getGroupBys());
        ret.select(jn.getColumnsSelected());
        ret.setLimitFrom(jn.getLimitFrom());
        ret.setLimitTo(jn.getLimitTo());
        ret.setOtherJoinOnFilter(FilterUtils.and(jn.getOtherJoinOnFilter(), FilterUtils.and(otherJoinOns)));
        Item unFoundSelFilter = makeRestFilter(charsetIndex);
        if (unFoundSelFilter != null)
            ret.setOtherJoinOnFilter(FilterUtils.and(ret.getOtherJoinOnFilter(), unFoundSelFilter));
        // and the origin where and the remain condition in selLists
        ret.having(jn.getHavingFilter());
        ret.setWhereFilter(jn.getWhereFilter());
        ret.setAlias(jn.getAlias());
        ret.setWithSubQuery(jn.isWithSubQuery());
        ret.setContainsSubQuery(jn.isContainsSubQuery());
        ret.setSql(jn.getSql());
        ret.setUpFields();
        return ret;
    }

    /**
     * tryMakeERJoin by  selList and join Unitss info
     *
     * @return
     */
    private JoinNode tryMakeERJoin(List<JoinColumnInfo> selList, int charsetIndex) {
        List<JoinColumnInfo> erKeys = new ArrayList<>();
        for (int i = 0; i < selList.size(); i++) {
            JoinColumnInfo jki = selList.get(i);
            if (jki.cm == null)
                continue;
            for (int j = i + 1; j < selList.size(); j++) {
                JoinColumnInfo jkj = selList.get(j);
                if (isErRelation(jki.cm, jkj.cm)) {
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

    // generate er join node ,remove jk of rKeyIndexs in selListIndex,replace the other selList's tn
    private JoinNode makeERJoin(List<JoinColumnInfo> erKeys, int charsetIndex) {
        PlanNode t0 = erKeys.get(0).tn;
        PlanNode t1 = erKeys.get(1).tn;
        JoinNode joinNode = new JoinNode(t0, t1, charsetIndex);
        List<ItemFuncEqual> joinFilter = makeJoinFilter(joinNode, t0, t1, true, charsetIndex);
        joinNode.setJoinFilter(joinFilter);
        for (int index = 2; index < erKeys.size(); index++) {
            t0 = joinNode;
            t1 = erKeys.get(index).tn;
            joinNode = new JoinNode(t0, t1, charsetIndex);
            joinFilter = makeJoinFilter(joinNode, t0, t1, true, charsetIndex);
            joinNode.setJoinFilter(joinFilter);
        }
        for (JoinColumnInfo jki : erKeys) {
            // remove join units
            for (int index = joinUnits.size() - 1; index > -1; index--) {
                PlanNode tn = joinUnits.get(index);
                if (tn == jki.tn)
                    joinUnits.remove(index);
            }
        }
        return joinNode;
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
            PlanNode tni = units.get(i);
            JoinNode joinNode = new JoinNode(ret, tni, charsetIndex);
            List<ItemFuncEqual> joinFilter = makeJoinFilter(joinNode, ret, tni, true, charsetIndex);
            joinNode.setJoinFilter(joinFilter);
            ret = joinNode;
        }
        return ret;
    }

    /**
     * join t0 and t1, find relation in selLists and make joinfilter
     * [if a.id=b.id and a.id = b.name would not appear]
     *
     * @param join
     * @param t0
     * @param t1
     * @param replaceSelList
     * @return
     */
    private List<ItemFuncEqual> makeJoinFilter(JoinNode join, PlanNode t0, PlanNode t1, boolean replaceSelList, int charsetIndex) {
        List<ItemFuncEqual> filters = new ArrayList<>();
        for (List<JoinColumnInfo> selList : selLists) {
            JoinColumnInfo jkit0 = null;
            JoinColumnInfo jkit1 = null;
            for (JoinColumnInfo jki : selList) {
                if (jki.tn == t0) {
                    jkit0 = jki;
                } else if (jki.tn == t1) {
                    jkit1 = jki;
                }
            }
            // t0and t1 in sel list can make jkit0 jkit1 join
            if (jkit0 != null && jkit1 != null) {
                JoinColumnInfo newJki = new JoinColumnInfo(jkit0.key);
                newJki.tn = join;
                if (jkit0.cm != null && jkit1.cm != null && isErRelation(jkit0.cm, jkit1.cm)) {
                    newJki.cm = jkit0.cm;
                    join.getERkeys().add(newJki.cm);
                }
                ItemFuncEqual bf = FilterUtils.equal(jkit0.key, jkit1.key, charsetIndex);
                filters.add(bf);
                selList.remove(jkit0);
                selList.remove(jkit1);
                selList.add(newJki);
            }
        }
        if (replaceSelList)
            replaceSelListReferedTn(t0, t1, join);
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
                List<ItemFuncEqual> jnFilter = makeJoinFilter(joinNode, newT, global, false, charsetIndex);
                // @if no join column, then the other is cross join
                if (jnFilter.size() > 0 || selLists.size() == 0) { // join
                    replaceSelListReferedTn(newT, global, joinNode);
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

    /**
     * try join t0 and global , the different to erjoin is ,we konw t0 can not be ER node
     *
     * @param t0
     * @param t1
     * @param join
     * @return
     */

    // change t0 and t1 in selList to join node
    private void replaceSelListReferedTn(PlanNode t0, PlanNode t1, PlanNode join) {
        for (List<JoinColumnInfo> list : selLists)
            for (JoinColumnInfo jki : list) {
                if (jki.tn == t0 || jki.tn == t1)
                    jki.tn = join;
            }
    }

    /**
     * init sellis's JoinColumnInfo,JoinColumnInfo only has key selList when just build ,
     * set values for them
     */
    private void initJoinColumnInfo() {
        for (List<JoinColumnInfo> selList : selLists) {
            for (JoinColumnInfo jki : selList) {
                for (PlanNode tn : joinUnits) {
                    Item tmpSel = nodeHasSelectable(tn, jki.key);
                    if (tmpSel != null) {
                        jki.tn = tn;
                        jki.cm = getERKey(tn, tmpSel);
                        break;
                    }
                }
            }
        }
    }

    /**
     * find the smallest join units in node
     *
     * @param node innerjoin
     */
    private void initInnerJoinUnits(JoinNode node) {
        if (isGlobalTree(node)) {
            this.globals.add(node);
        } else {
            for (int index = 0; index < node.getChildren().size(); index++) {
                PlanNode child = node.getChildren().get(index);
                if (isUnit(child)) {
                    child = JoinERProcessor.optimize(child);
                    node.getChildren().set(index, child);
                    this.joinUnits.add(child);
                } else {
                    initInnerJoinUnits((JoinNode) child);
                }
            }
        }
    }

    private boolean isUnit(PlanNode node) {
        if (isGlobalTree(node))
            return true;
        else return node.type() != PlanNode.PlanNodeType.JOIN || node.isWithSubQuery() || !((JoinNode) node).isInnerJoin();
    }

    /**
     * visitJoinOns
     *
     * @param joinNode
     */
    private void visitJoinOns(JoinNode joinNode) {
        for (PlanNode unit : joinUnits) {
            // is unit
            if (unit == joinNode) {
                return;
            }
        }

        for (ItemFuncEqual filter : joinNode.getJoinFilter()) {
            addJoinFilter(filter);
        }

        for (PlanNode child : joinNode.getChildren()) {
            if ((!isUnit(child)) && (child.type().equals(PlanNode.PlanNodeType.JOIN))) {
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
     * parser joinfilter ,add key to selLists
     *
     * @param filter
     */
    private void addJoinFilter(ItemFuncEqual filter) {
        Item left = filter.arguments().get(0);
        Item right = filter.arguments().get(1);
        JoinColumnInfo jiLeft = new JoinColumnInfo(left);
        JoinColumnInfo jiRight = new JoinColumnInfo(right);
        for (int i = 0; i < selLists.size(); i++) {
            List<JoinColumnInfo> equalSelectables = selLists.get(i);
            if (equalSelectables.contains(jiLeft)) {
                addANewKey(jiRight, i);
                return;
            } else if (equalSelectables.contains(jiRight)) {
                addANewKey(jiLeft, i);
                return;
            }
        }
        ArrayList<JoinColumnInfo> equalSelectables = new ArrayList<>();
        equalSelectables.add(jiLeft);
        equalSelectables.add(jiRight);
        selLists.add(equalSelectables);
        for (int i = selLists.size() - 1; i > -1; i--) {
            List<JoinColumnInfo> list = selLists.get(i);
            if (list.size() == 0)
                selLists.remove(i);
        }
    }

    /**
     * put an new ISelectable into selLists ,
     * eg: insert sellist[0] test3.id,test2.id
     * sellist[1] test4.id,test5.id,test6.id firstly
     * now there is a joinfilter is test3.id=test4.id,
     * we must put test4.id into sellist[0] ,add put all sellist[1]'s item into sel[0]
     *
     * @param s
     * @param listIndex
     */
    private void addANewKey(JoinColumnInfo s, int listIndex) {
        List<JoinColumnInfo> equalSelectables = selLists.get(listIndex);
        for (int i = 0; i < selLists.size(); i++) {
            if (i != listIndex) {
                List<JoinColumnInfo> list = selLists.get(i);
                if (list.contains(s)) {
                    equalSelectables.addAll(list);
                    list.clear();
                }
            }
        }
        if (!equalSelectables.contains(s))
            equalSelectables.add(s);
    }

    /**
     * the origin filter is  join on filter
     * ,now join is changed ,and the filter is not join filter any more ,add it to other join on
     *
     * @return
     */
    private Item makeRestFilter(int charsetIndex) {
        Item filter = null;
        for (List<JoinColumnInfo> selList : selLists) {
            if (selList.size() > 2) {
                Item sel0 = selList.get(0).key;
                for (int index = 1; index < selList.size(); index++) {
                    ItemFuncEqual bf = FilterUtils.equal(sel0, selList.get(index).key, charsetIndex);
                    filter = FilterUtils.and(filter, bf);
                }
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
        ERTable cm = new ERTable(tableNode.getSchema(), tableNode.getPureName(), col.getItemName());
        if (tn.type() == PlanNode.PlanNodeType.TABLE) {
            return cm;
        } else {
            List<ERTable> erList = ((JoinNode) tn).getERkeys();
            for (ERTable cerKey : erList) {
                if (isErRelation(cm, cerKey))
                    return cm;
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

    private Item nodeHasSelectable(PlanNode child, Item sel) {
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
                Item argSel = nodeHasSelectable(child, arg);
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

    /**
     * JoinColumnInfo
     *
     * @author ActionTech
     */
    private static class JoinColumnInfo {
        private Item key; // join on's on key
        private PlanNode tn; // treenode of the joinkey belong to
        private ERTable cm; //  joinkey is er ,if so,save th parentkey

        JoinColumnInfo(Item key) {
            this.key = key;
            tn = null;
            cm = null;
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

        public PlanNode getTn() {
            return tn;
        }

        public void setTn(PlanNode tn) {
            this.tn = tn;
        }

        public ERTable getCm() {
            return cm;
        }

        public void setCm(ERTable cm) {
            this.cm = cm;
        }
    }
}
