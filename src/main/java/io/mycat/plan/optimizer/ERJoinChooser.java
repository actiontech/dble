package io.mycat.plan.optimizer;

import io.mycat.MycatServer;
import io.mycat.config.model.ERTable;
import io.mycat.plan.NamedField;
import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.TableNode;
import io.mycat.plan.util.FilterUtils;
import io.mycat.plan.util.PlanUtil;
import io.mycat.route.parser.util.Pair;
import io.mycat.util.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ERJoinChooser {
    /**
     * 保存整个join树的join关系 ex: t1 inner join t2 on t1.id=t2.id inner join t3 on
     * t1.name=t3.name and t1.id=t3.id 则 sel[0]: t1.id,t2.id,t3.id sel[1]:
     * t1.name,t3.name
     */
    private List<ArrayList<JoinKeyInfo>> selLists = new ArrayList<ArrayList<JoinKeyInfo>>();

    // 当前正在对selLists的那一行做er尝试
    private int trySelListIndex = 0;

    private List<PlanNode> joinUnits = new ArrayList<PlanNode>();

    // global表
    private List<PlanNode> globals = new ArrayList<PlanNode>();

    // 生成的若干的er joinnode
    private List<JoinNode> makedERJnList = new ArrayList<JoinNode>();

    private List<Item> otherJoinOns = new ArrayList<Item>();

    private JoinNode jn = null;

    private Map<ERTable, Set<ERTable>> erRelations;

    public ERJoinChooser(JoinNode qtn, Map<ERTable, Set<ERTable>> erRelations) {
        this.jn = qtn;
        this.erRelations = erRelations;
    }

    public ERJoinChooser(JoinNode qtn) {
        this(qtn, MycatServer.getInstance().getConfig().getErRelations());
    }

    public JoinNode optimize() {
        if (jn.isLeftOuterJoin() && !jn.isNotIn()) {
            return leftJoinOptimizer();
        } else { // (jn.isInnerJoin()) {
            return innerJoinOptimizer();
        }
    }

    /* ------------------- left join optimizer start -------------------- */

    /**
     * left join时也可以进行ER优化，但是策略和inner join不同
     * ex:t1，t2中，t1.id和t2.id是外键关联，且是拆分ER规则(同一个拆分规则)，那么t1 left join t2 on
     * t1.id=t2.id是可以下发的 <leftjoin我们不更改join节点的结构，仅是做出判断，该left join是否可以ER优化>
     *
     * @return
     */
    private JoinNode leftJoinOptimizer() {
        PlanNode left = jn.getLeftNode();
        PlanNode right = jn.getRightNode();
        if (left.type() == PlanNodeType.JOIN) {
            left = JoinERProcessor.optimize(left);
            jn.setLeftNode(left);
        }
        if (right.type() == PlanNodeType.JOIN) {
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

    private ERTable getLeftOutJoinChildER(JoinNode jn, PlanNode child, Item onItem) {
        if (PlanUtil.existAggr(child))
            return null;
        else if (!PlanUtil.isERNode(child) && child.type() != PlanNodeType.TABLE)
            return null;
        if (onItem == null || !onItem.type().equals(ItemType.FIELD_ITEM))
            return null;
        Pair<TableNode, ItemField> joinColumnInfo = PlanUtil.findColumnInTableLeaf((ItemField) onItem, jn);
        if (joinColumnInfo == null)
            return null;
        TableNode tn = joinColumnInfo.getKey();
        ItemField col = joinColumnInfo.getValue();
        ERTable erKey = new ERTable(tn.getSchema(), tn.getPureName(), col.getItemName());
        if (child.type() == PlanNodeType.TABLE) {
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
     * inner join的er优化，将inner join的unit拆分进行重新拼接
     *
     * @return
     */
    private JoinNode innerJoinOptimizer() {
        initInnerJoinUnits(jn);
        if (joinUnits.size() == 1) {
            return jn;
        }
        visitJoinOns(jn);
        initJoinKeyInfo();
        while (trySelListIndex < selLists.size()) {
            List<JoinKeyInfo> selList = selLists.get(trySelListIndex);
            JoinNode erJoinNode = tryMakeERJoin(selList);
            if (erJoinNode == null) {
                trySelListIndex++;
            } else {
                // 重新扫描
                this.makedERJnList.add(erJoinNode);
            }
        }
        if (makedERJnList.isEmpty())
            // 未发现er关系join
            return jn;

        List<PlanNode> others = new ArrayList<PlanNode>();
        // makedErJnList放在前面，这样可以和ER进行join
        others.addAll(makedERJnList);
        others.addAll(joinUnits);
        for (int i = 0; i < others.size(); i++) {
            // 这个地方需要将这些无法再进一步优化的unit尽量和global组合
            PlanNode tnewOther = others.get(i);
            PlanNode newT0 = joinWithGlobal(tnewOther, globals);
            others.set(i, newT0);
        }
        // 到这儿为止，就剩下others和globals中有可能有node了，而且other中的和global中的已经都尝试过join了
        if (globals.size() > 0) {
            PlanNode globalJoin = makeJoinNode(globals);
            others.add(globalJoin);
        }
        // others中的节点都是无法进行优化的join units，随意将他们进行组合即可
        JoinNode ret = (JoinNode) makeJoinNode(others);
        ret.setOrderBys(jn.getOrderBys());
        ret.setGroupBys(jn.getGroupBys());
        ret.select(jn.getColumnsSelected());
        ret.setLimitFrom(jn.getLimitFrom());
        ret.setLimitTo(jn.getLimitTo());
        ret.setOtherJoinOnFilter(FilterUtils.and(jn.getOtherJoinOnFilter(), FilterUtils.and(otherJoinOns)));
        Item unFoundSelFilter = makeRestFilter();
        if (unFoundSelFilter != null)
            ret.setOtherJoinOnFilter(FilterUtils.and(ret.getOtherJoinOnFilter(), unFoundSelFilter));
        // 需要将原来的where以及selLists中未处理掉的条件and起来
        ret.having(jn.getHavingFilter());
        ret.setWhereFilter(jn.getWhereFilter());
        ret.setAlias(jn.getAlias());
        ret.setSubQuery(jn.isSubQuery());
        ret.setSql(jn.getSql());
        ret.setUpFields();
        return (JoinNode) ret;
    }

    /**
     * 根据selList以及joinUnits的信息尝试生成ERJoin
     *
     * @return
     */
    private JoinNode tryMakeERJoin(List<JoinKeyInfo> selList) {
        List<JoinKeyInfo> erKeys = new ArrayList<JoinKeyInfo>();
        for (int i = 0; i < selList.size(); i++) {
            JoinKeyInfo jki = selList.get(i);
            if (jki.cm == null)
                continue;
            for (int j = i + 1; j < selList.size(); j++) {
                JoinKeyInfo jkj = selList.get(j);
                if (isErRelation(jki.cm, jkj.cm)) {
                    erKeys.add(jkj);
                }
            }
            if (!erKeys.isEmpty()) {
                // er found
                erKeys.add(0, jki);
                return makeERJoin(erKeys);
            }
        }
        return null;
    }

    // 生成er join节点，删除selListIndex中的erKeyIndexs的jk，替换其余selList中的tn节点
    private JoinNode makeERJoin(List<JoinKeyInfo> erKeys) {
        PlanNode t0 = erKeys.get(0).tn;
        PlanNode t1 = erKeys.get(1).tn;
        JoinNode jn = new JoinNode(t0, t1);
        List<ItemFuncEqual> joinFilter = makeJoinFilter(jn, t0, t1, true);
        jn.setJoinFilter(joinFilter);
        for (int index = 2; index < erKeys.size(); index++) {
            t0 = jn;
            t1 = erKeys.get(index).tn;
            jn = new JoinNode(t0, t1);
            joinFilter = makeJoinFilter(jn, t0, t1, true);
            jn.setJoinFilter(joinFilter);
        }
        for (JoinKeyInfo jki : erKeys) {
            // remove join units
            for (int index = joinUnits.size() - 1; index > -1; index--) {
                PlanNode tn = joinUnits.get(index);
                if (tn == jki.tn)
                    joinUnits.remove(index);
            }
        }
        return jn;
    }

    /**
     * 将units中的join节点随意组合生成join节点
     *
     * @param units
     * @return
     */
    private PlanNode makeJoinNode(List<PlanNode> units) {
        PlanNode ret = units.get(0);
        for (int i = 1; i < units.size(); i++) {
            PlanNode tni = units.get(i);
            JoinNode jn = new JoinNode(ret, tni);
            List<ItemFuncEqual> joinFilter = makeJoinFilter(jn, ret, tni, true);
            jn.setJoinFilter(joinFilter);
            ret = jn;
        }
        return ret;
    }

    /**
     * t0和t1进行join，在selLists中查找他们之间存在的join关系，并将这些关系进行组合形成joinfilter
     * [假设不会出现a.id=b.id and a.id = b.name这种无意义的join语句]
     *
     * @param tmp
     * @param t0
     * @param ti
     * @return
     */
    private List<ItemFuncEqual> makeJoinFilter(JoinNode join, PlanNode t0, PlanNode t1, boolean replaceSelList) {
        List<ItemFuncEqual> filters = new ArrayList<ItemFuncEqual>();
        for (List<JoinKeyInfo> selList : selLists) {
            JoinKeyInfo jkit0 = null;
            JoinKeyInfo jkit1 = null;
            for (int i = 0; i < selList.size(); i++) {
                JoinKeyInfo jki = selList.get(i);
                if (jki.tn == t0) {
                    jkit0 = jki;
                } else if (jki.tn == t1) {
                    jkit1 = jki;
                }
            }
            // 这一行sellist里t0和t1可以按照jkit0 jkit1 join
            if (jkit0 != null && jkit1 != null) {
                JoinKeyInfo newJki = new JoinKeyInfo(jkit0.key);
                newJki.tn = join;
                if (jkit0.cm != null && jkit1.cm != null && isErRelation(jkit0.cm, jkit1.cm)) {
                    newJki.cm = jkit0.cm;
                    join.getERkeys().add(newJki.cm);
                }
                ItemFuncEqual bf = FilterUtils.equal(jkit0.key, jkit1.key);
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

    private PlanNode joinWithGlobal(PlanNode t, List<PlanNode> globals) {
        PlanNode newT = t;
        while (globals.size() > 0) {
            boolean foundJoin = false;
            for (int i = 0; i < globals.size(); i++) {
                PlanNode global = globals.get(i);
                // 尝试做他们的join
                JoinNode jn = new JoinNode(newT, global);
                List<ItemFuncEqual> jnFilter = makeJoinFilter(jn, newT, global, false);
                // @如果没有可以join的join列，证明剩下的都是cross join
                if (jnFilter.size() > 0 || selLists.size() == 0) { // 可以join
                    replaceSelListReferedTn(newT, global, jn);
                    foundJoin = true;
                    jn.setJoinFilter(jnFilter);
                    globals.remove(i);
                    newT = jn;
                    break;
                }
            }
            if (!foundJoin) // no join can do from t and globals
                break;
        }
        return newT;
    }

    /**
     * 尝试着对t0和global的节点进行join，和erjoin不同的是，t0是已经被er优化唰下来的，不可以再ER优化了
     *
     * @param t0
     * @param global
     * @return
     */

    // 将selList中引用到t0以及t1的节点改成引用join节点
    private void replaceSelListReferedTn(PlanNode t0, PlanNode t1, PlanNode join) {
        for (List<JoinKeyInfo> list : selLists)
            for (JoinKeyInfo jki : list) {
                if (jki.tn == t0 || jki.tn == t1)
                    jki.tn = join;
            }
    }

    /**
     * 初始化sellist中的JoinKeyInfo,selList刚组建好时，里面的JoinKeyInfo只有key属性，这个函数
     * 就是对其它的两个属性赋值
     */
    private void initJoinKeyInfo() {
        for (List<JoinKeyInfo> selList : selLists) {
            for (JoinKeyInfo jki : selList) {
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
     * 查找node中最小的join units
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
        else if (node.type() == PlanNodeType.JOIN && !node.isSubQuery() && ((JoinNode) node).isInnerJoin())
            // 仅有干净的inner join才可以作为继续拆分的对象
            return false;
        else
            return true;
    }

    /**
     * 遍历node
     *
     * @param node
     */
    private void visitJoinOns(JoinNode jn) {
        for (PlanNode unit : joinUnits) {
            // 已经是最小节点
            if (unit == jn) {
                return;
            }
        }

        for (ItemFuncEqual filter : jn.getJoinFilter()) {
            addJoinFilter(filter);
        }

        for (PlanNode child : jn.getChildren()) {
            if ((!isUnit(child)) && (child.type().equals(PlanNodeType.JOIN))) {
                // a join b on a.id=b.id and a.id+b.id=10 join c on
                // a.id=c.id，将a.id+b.id提上来
                JoinNode jnChild = (JoinNode) child;
                if (jnChild.getOtherJoinOnFilter() != null)
                    otherJoinOns.add(jnChild.getOtherJoinOnFilter());
                visitJoinOns((JoinNode) child);
            }
        }
    }

    /**
     * 将joinfilter串解析，得到的key放置到selLists中
     *
     * @param filter
     */
    private void addJoinFilter(ItemFuncEqual filter) {
        Item left = filter.arguments().get(0);
        Item right = filter.arguments().get(1);
        JoinKeyInfo jiLeft = new JoinKeyInfo(left);
        JoinKeyInfo jiRight = new JoinKeyInfo(right);
        for (int i = 0; i < selLists.size(); i++) {
            List<JoinKeyInfo> equalSelectables = selLists.get(i);
            if (equalSelectables.contains(jiLeft)) {
                addANewKey(jiRight, i);
                return;
            } else if (equalSelectables.contains(jiRight)) {
                addANewKey(jiLeft, i);
                return;
            }
        }
        ArrayList<JoinKeyInfo> equalSelectables = new ArrayList<JoinKeyInfo>();
        equalSelectables.add(jiLeft);
        equalSelectables.add(jiRight);
        selLists.add(equalSelectables);
        for (int i = selLists.size() - 1; i > -1; i--) {
            List<JoinKeyInfo> list = selLists.get(i);
            if (list.size() == 0)
                selLists.remove(i);
        }
    }

    /**
     * 将一个新的ISelectable放置到selLists中，因为存在这种情况： 先插入的是 sellist[0] test3.id,test2.id
     * sellist[1] test4.id,test5.id,test6.id
     * 这个时候如果有joinfilter是test3.id=test4.id，
     * 显然需要将test4.id放置到sellist[0]中，同时，要将sellist[1] 中剩余的项也放置到sel[0]中
     *
     * @param s
     * @param listIndex
     */
    private void addANewKey(JoinKeyInfo s, int listIndex) {
        List<JoinKeyInfo> equalSelectables = selLists.get(listIndex);
        for (int i = 0; i < selLists.size(); i++) {
            if (i != listIndex) {
                List<JoinKeyInfo> list = selLists.get(i);
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
     * 由于join节点的变化，有可能导致有一些filter原来是join on
     * filter的，但是现在成为不了joinfilter了，把他们丢到other join on中
     *
     * @return
     */
    private Item makeRestFilter() {
        Item filter = null;
        for (List<JoinKeyInfo> selList : selLists) {
            if (selList.size() > 2) {
                Item sel0 = selList.get(0).key;
                for (int index = 1; index < selList.size(); index++) {
                    ItemFuncEqual bf = FilterUtils.equal(sel0, selList.get(index).key);
                    filter = FilterUtils.and(filter, bf);
                }
            }
        }
        return filter;
    }

    /**
     * 记录joinkey的关联属性
     *
     * @author chenzifei
     */
    private static class JoinKeyInfo {
        public Item key; // join on的on key
        public PlanNode tn; // 该joinkey属于哪个treenode
        public ERTable cm; // 该joinkey是否有er关联，如果有er关联的话，保存它的parentkey

        public JoinKeyInfo(Item key) {
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
            if (!(o instanceof JoinKeyInfo)) {
                return false;
            }
            JoinKeyInfo other = (JoinKeyInfo) o;
            if (this.key == null)
                return false;
            return StringUtil.equals(this.key.getTableName(), other.key.getTableName()) &&
                    StringUtil.equalsIgnoreCase(this.key.getItemName(), other.key.getItemName());
        }

        @Override
        public String toString() {
            return "key:" + key;
        }
    }

    private ERTable getERKey(PlanNode tn, Item c) {
        if (!(c instanceof ItemField))
            return null;
        if (tn.type() != PlanNodeType.TABLE && !PlanUtil.isERNode(tn)) {
            return null;
        }
        Pair<TableNode, ItemField> pair = PlanUtil.findColumnInTableLeaf((ItemField) c, tn);
        if (pair == null)
            return null;
        TableNode tableNode = pair.getKey();
        ItemField col = pair.getValue();
        ERTable cm = new ERTable(tableNode.getSchema(), tableNode.getPureName(), col.getItemName());
        if (tn.type() == PlanNodeType.TABLE) {
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
        if (tn instanceof TableNode) {
            return tn.getUnGlobalTableCount() == 0 ? true : false;
        } else if (tn.type() == PlanNodeType.NONAME) {
            return true;
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
        } else if (sel.type().equals(ItemType.SUM_FUNC_ITEM)) {
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
            // 存在col名称
            if (child.getAlias() == null) {
                for (Entry<NamedField, Item> entry : child.getOuterFields().entrySet()) {
                    if (StringUtil.equals(table, entry.getKey().getTable()) &&
                            StringUtil.equalsIgnoreCase(colName, entry.getKey().getName())) {
                        return entry.getValue();
                    }
                }
                return null;
            } else {
                if (!StringUtil.equals(table, child.getAlias()))
                    return null;
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
}
