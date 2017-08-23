package io.mycat.plan.node;

import io.mycat.config.ErrorCode;
import io.mycat.config.model.ERTable;
import io.mycat.plan.NamedField;
import io.mycat.plan.Order;
import io.mycat.plan.PlanNode;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import io.mycat.plan.util.FilterUtils;
import io.mycat.plan.util.PlanUtil;
import io.mycat.plan.util.ToStringUtil;
import io.mycat.util.StringUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class JoinNode extends PlanNode {

    public PlanNodeType type() {
        return PlanNodeType.JOIN;
    }


    public enum Strategy {
        SORTMERGE, NESTLOOP
    }

    /**
     * 是否是not in
     */
    private boolean isNotIn = false;

    public boolean isNotIn() {
        return isNotIn;
    }

    public void setNotIn(boolean isNotIn) {
        this.isNotIn = isNotIn;
    }

    // sort-merge-join时左节点的排序属性
    private List<Order> leftJoinOnOrders = new ArrayList<Order>();
    private boolean isLeftOrderMatch = false;
    // sort-merge-join时右节点的排序属性
    private List<Order> rightJoinOnOrders = new ArrayList<Order>();
    private boolean isRightOrderMatch = false;

    private List<String> usingFields;

    private boolean isNatural = false;
    /**
     * <pre>
     * leftOuterJoin:
     *      leftOuter=true && rightOuter=false
     * rightOuterJoin:
     *      leftOuter=false && rightOuter=true
     * innerJoin:
     *      leftOuter=false && rightOuter=false
     * outerJoin:
     *      leftOuter=true && rightOuter=true
     * </pre>
     */
    private boolean leftOuter;
    private boolean rightOuter;
    private boolean needOptimizeJoinOrder;
    private List<ItemFuncEqual> joinFilter;
    private Item otherJoinOnFilter;

    /**
     * 虚拟化一个节点可以被ER关联的key代表
     */
    protected List<ERTable> erKeys = new ArrayList<ERTable>();

    protected Strategy strategy = Strategy.SORTMERGE;

    public JoinNode() {
        this.leftOuter = false;
        this.rightOuter = false;
        this.needOptimizeJoinOrder = true;
        this.joinFilter = new ArrayList<ItemFuncEqual>();
    }

    public JoinNode(PlanNode left, PlanNode right) {
        this();
        addChild(left);
        addChild(right);
    }

    @Override
    public void setUpFields() {
        super.setUpFields();
        buildJoinFilters();
        buildOtherJoinOn();
        buildJoinKeys(false);
    }

    private void buildJoinFilters() {
        nameContext.setFindInSelect(false);
        nameContext.setSelectFirst(false);

        if (usingFields != null) {
            for (String using : usingFields) {
                using = StringUtil.removeBackQuote(using);
                String lName = findTbNameByUsing(this.getLeftNode(), using);
                String rName = findTbNameByUsing(this.getRightNode(), using);
                if (lName.equals(rName)) {
                    throw new MySQLOutPutException(ErrorCode.ER_NONUNIQ_TABLE, "42000", "Not unique table/alias: '" + lName + "'");
                }
                Item filter = setUpItem(genJoinFilter(using, lName, rName));
                joinFilter.add((ItemFuncEqual) filter);
            }
        } else {
            for (int index = 0; index < joinFilter.size(); index++) {
                Item bf = joinFilter.get(index);
                bf = setUpItem(bf);
                if (bf.getReferTables().size() == 1) {
                    throw new MySQLOutPutException(ErrorCode.ER_NONUNIQ_TABLE, "42000", "Not unique table/alias: '" + this.getLeftNode().getPureName() + "'");
                }
                joinFilter.set(index, (ItemFuncEqual) bf);
            }
        }
    }

    public void genUsingByNatural() {
        List<String> using = getFieldList(this.getLeftNode());
        List<String> rightFiled = getFieldList(this.getRightNode());
        using.retainAll(rightFiled);
        if (using.size() > 0) {
            this.setUsingFields(using);
        }
    }

    private List<String> getFieldList(PlanNode node) {
        List<String> fields = new ArrayList<>();
        Set<String> checkDup = new HashSet<>();
        for (NamedField field : node.getOuterFields().keySet()) {
            String fieldName = field.getName().toLowerCase();
            if (checkDup.contains(fieldName)) {
                throw new MySQLOutPutException(ErrorCode.ER_DUP_FIELDNAME, "42S21", " Duplicate column name '" + fieldName + "'");
            }
            checkDup.add(fieldName);
            fields.add(fieldName);
        }
        return fields;
    }

    private String findTbNameByUsing(PlanNode node, String using) {
        String table = node.getCombinedName();
        if (table != null) {
            return table;
        }
        boolean found = false;
        for (NamedField field : node.getOuterFields().keySet()) {
            if (field.getName().equalsIgnoreCase(using)) {
                if (!found) {
                    found = true;
                    table = field.getTable();
                } else {
                    throw new MySQLOutPutException(ErrorCode.ER_NON_UNIQ_ERROR, "23000",
                            " Column '" + using + "' in from clause is ambiguous");
                }
            }
        }
        return table;
    }

    private ItemFuncEqual genJoinFilter(String using, String leftJoinNode, String rightJoinNode) {
        ItemField column1 = new ItemField(null, leftJoinNode, using);
        ItemField column2 = new ItemField(null, rightJoinNode, using);
        return new ItemFuncEqual(column1, column2);
    }

    private void buildOtherJoinOn() {
        nameContext.setFindInSelect(false);
        nameContext.setSelectFirst(false);
        if (otherJoinOnFilter != null)
            otherJoinOnFilter = setUpItem(otherJoinOnFilter);
    }

    public List<String> getUsingFields() {
        return usingFields;
    }

    public void setUsingFields(List<String> usingFields) {
        this.usingFields = usingFields;
    }

    public boolean isNatural() {
        return isNatural;
    }

    public void setNatural(boolean natural) {
        isNatural = natural;
    }

    @Override
    protected void dealSingleStarColumn(List<Item> newSels) {
        if (usingFields == null) {
            super.dealSingleStarColumn(newSels);
        } else {
            PlanNode driverNode = this.isRightOuterJoin() ? this.getRightNode() : this.getLeftNode();
            String table = findTbNameByUsing(driverNode, usingFields.get(0));
            for (String fieldName : usingFields) {
                ItemField col = new ItemField(null, table, fieldName);
                newSels.add(col);
            }
            for (NamedField field : innerFields.keySet()) {
                if (usingFields.contains(field.getName())) {
                    continue;
                }
                ItemField col = new ItemField(null, field.getTable(), field.getName());
                newSels.add(col);
            }
        }
    }

    /**
     * setupJoinfilters
     *
     * @param clearName if true:clear filter's itemname,else keep
     */
    private void buildJoinKeys(boolean clearName) {
        List<Item> otherJoinOnFilters = new ArrayList<Item>(getJoinFilter().size());
        for (int index = 0; index < joinFilter.size(); index++) {
            ItemFuncEqual bf = joinFilter.get(index);
            if (clearName)
                bf.setItemName(null);
            boolean isJoinKey = PlanUtil.isJoinKey(bf, this);
            if (!isJoinKey) {
                otherJoinOnFilters.add(bf);
                otherJoinOnFilter = FilterUtils.and(otherJoinOnFilter, bf);
            }
        }
        getJoinFilter().removeAll(otherJoinOnFilters);
    }

    public List<Item> getLeftKeys() {
        List<Item> leftKeys = new ArrayList<Item>(this.getJoinFilter().size());
        for (ItemFuncEqual f : this.getJoinFilter()) {
            leftKeys.add(f.arguments().get(0));
        }
        return leftKeys;
    }

    public List<Item> getRightKeys() {
        List<Item> rightKeys = new ArrayList<Item>(this.getJoinFilter().size());
        for (ItemFuncEqual f : this.getJoinFilter()) {
            rightKeys.add(f.arguments().get(1));
        }
        return rightKeys;
    }

    public JoinNode addJoinKeys(Item leftKey, Item rightKey) {
        this.joinFilter.add(FilterUtils.equal(leftKey, rightKey));
        return this;
    }

    public void addJoinFilter(ItemFuncEqual filter) {
        this.joinFilter.add(filter);
    }

    public PlanNode getLeftNode() {
        if (children.isEmpty())
            return null;
        return children.get(0);

    }

    public PlanNode getRightNode() {
        if (children.size() < 2)
            return null;
        return children.get(1);
    }

    public void setLeftNode(PlanNode left) {
        if (children.isEmpty()) {
            addChild(left);
        } else {
            children.set(0, left);
            left.setParent(this);
        }
    }

    public void setRightNode(PlanNode right) {
        if (this.getChildren().isEmpty()) {
            addChild(null);
        }
        if (this.getChildren().size() == 1) {
            addChild(right);
        } else {
            children.set(1, right);
            right.setParent(this);
        }

    }

    public String getPureName() {
        return null;
    }

    /**
     * 交换左右节点
     */
    public void exchangeLeftAndRight() {

        PlanNode tmp = this.getLeftNode();
        this.setLeftNode(this.getRightNode());
        this.setRightNode(tmp);

        boolean tmpouter = this.leftOuter;
        this.leftOuter = this.rightOuter;
        this.rightOuter = tmpouter;

        this.buildJoinKeys(true);

    }

    public List<ItemFuncEqual> getJoinFilter() {
        return this.joinFilter;
    }

    public void setJoinFilter(List<ItemFuncEqual> joinFilter) {
        this.joinFilter = joinFilter;
    }

    public JoinNode setLeftOuterJoin() {
        this.leftOuter = true;
        this.rightOuter = false;
        return this;
    }

    public JoinNode setRightOuterJoin() {
        this.rightOuter = true;
        this.leftOuter = false;
        return this;
    }

    public JoinNode setInnerJoin() {
        this.leftOuter = false;
        this.rightOuter = false;
        return this;
    }

    public boolean getLeftOuter() {
        return this.leftOuter;
    }

    public boolean getRightOuter() {
        return this.rightOuter;
    }

    public boolean isLeftOuterJoin() {
        return (this.getLeftOuter()) && (!this.getRightOuter());
    }

    public boolean isRightOuterJoin() {
        return (!this.getLeftOuter()) && (this.getRightOuter());
    }

    public boolean isInnerJoin() {
        return (!this.getLeftOuter()) && (!this.getRightOuter());
    }


    public boolean isNeedOptimizeJoinOrder() {
        return this.needOptimizeJoinOrder;
    }

    public void setNeedOptimizeJoinOrder(boolean needOptimizeJoinOrder) {
        this.needOptimizeJoinOrder = needOptimizeJoinOrder;
    }

    public JoinNode setLeftRightJoin(boolean leftOuter, boolean rightOuter) {
        this.leftOuter = leftOuter;
        this.rightOuter = rightOuter;
        return this;
    }

    public List<Order> getLeftJoinOnOrders() {
        return leftJoinOnOrders;
    }

    public List<Order> getRightJoinOnOrders() {
        return rightJoinOnOrders;
    }

    @Override
    public JoinNode copy() {
        JoinNode newJoinNode = new JoinNode();
        this.copySelfTo(newJoinNode);
        newJoinNode.setJoinFilter(new ArrayList<ItemFuncEqual>());
        for (Item bf : joinFilter) {
            newJoinNode.addJoinFilter((ItemFuncEqual) bf.cloneStruct());
        }
        newJoinNode.setLeftNode((PlanNode) this.getLeftNode().copy());
        newJoinNode.setRightNode((PlanNode) this.getRightNode().copy());
        newJoinNode.setNeedOptimizeJoinOrder(this.isNeedOptimizeJoinOrder());
        newJoinNode.leftOuter = this.leftOuter;
        newJoinNode.rightOuter = this.rightOuter;
        newJoinNode.isNotIn = this.isNotIn;
        newJoinNode.otherJoinOnFilter = this.otherJoinOnFilter == null ? null : this.otherJoinOnFilter.cloneItem();
        return newJoinNode;
    }

    public List<ERTable> getERkeys() {
        return this.erKeys;
    }

    @Override
    public int getHeight() {
        int maxChildHeight = 0;
        for (PlanNode child : children) {
            int childHeight = child.getHeight();
            if (childHeight > maxChildHeight)
                maxChildHeight = childHeight;
        }
        return maxChildHeight + 1;
    }

    /**
     * @return the isLeftOrderMatch
     */
    public boolean isLeftOrderMatch() {
        return isLeftOrderMatch;
    }

    /**
     * @param isLeftOrderMatch the isLeftOrderMatch to set
     */
    public void setLeftOrderMatch(boolean isLeftOrderMatch) {
        this.isLeftOrderMatch = isLeftOrderMatch;
    }

    /**
     * @return the isRightOrderMatch
     */
    public boolean isRightOrderMatch() {
        return isRightOrderMatch;
    }

    /**
     * @param isRightOrderMatch the isRightOrderMatch to set
     */
    public void setRightOrderMatch(boolean isRightOrderMatch) {
        this.isRightOrderMatch = isRightOrderMatch;
    }

    /**
     * @return the strategy
     */
    public Strategy getStrategy() {
        return strategy;
    }

    /**
     * @param strategy the strategy to set
     */
    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    public Item getOtherJoinOnFilter() {
        return otherJoinOnFilter;
    }

    public void setOtherJoinOnFilter(Item otherJoinOnFilter) {
        this.otherJoinOnFilter = otherJoinOnFilter;
    }

    @Override
    public String toString(int level) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = ToStringUtil.getTab(level);
        String tabContent = ToStringUtil.getTab(level + 1);
        if (this.getAlias() != null) {
            ToStringUtil.appendln(sb, tabTittle + "Join" + " as " + this.getAlias());
        } else {
            ToStringUtil.appendln(sb, tabTittle + "Join");
        }
        ToStringUtil.appendln(sb, tabContent + "joinStrategy: " + this.getStrategy());
        if (this.isInnerJoin()) {
            ToStringUtil.appendln(sb, tabContent + "type: " + "inner join");
        } else if (this.isRightOuterJoin()) {
            ToStringUtil.appendln(sb, tabContent + "type: " + "right outter join");
        } else if (this.isLeftOuterJoin()) {
            ToStringUtil.appendln(sb, tabContent + "type: " + "left outter join");
        }
        ToStringUtil.appendln(sb, tabContent + "joinFilter: " + ToStringUtil.itemListString(this.getJoinFilter()));
        ToStringUtil.appendln(sb, tabContent + "otherJoinOnFilter: " + ToStringUtil.itemString(this.otherJoinOnFilter));
        ToStringUtil.appendln(sb, tabContent + "leftJoinOnOrder: " + ToStringUtil.orderListString(leftJoinOnOrders));
        ToStringUtil.appendln(sb, tabContent + "rightJoinOnOrder: " + ToStringUtil.orderListString(rightJoinOnOrders));
        ToStringUtil.appendln(sb, tabContent + "isDistinct: " + isDistinct());
        ToStringUtil.appendln(sb, tabContent + "columns: " + ToStringUtil.itemListString(columnsSelected));
        ToStringUtil.appendln(sb, tabContent + "where: " + ToStringUtil.itemString(whereFilter));
        ToStringUtil.appendln(sb, tabContent + "having: " + ToStringUtil.itemString(havingFilter));
        ToStringUtil.appendln(sb, tabContent + "groupBy: " + ToStringUtil.orderListString(groups));
        ToStringUtil.appendln(sb, tabContent + "orderBy: " + ToStringUtil.orderListString(orderBys));
        if (this.getLimitFrom() >= 0L && this.getLimitTo() > 0L) {
            ToStringUtil.appendln(sb, tabContent + "limitFrom: " + this.getLimitFrom());
            ToStringUtil.appendln(sb, tabContent + "limitTo: " + this.getLimitTo());
        }
        ToStringUtil.appendln(sb, tabContent + "sql: " + this.getSql());

        ToStringUtil.appendln(sb, tabContent + "left:");
        sb.append(this.getLeftNode().toString(level + 2));
        ToStringUtil.appendln(sb, tabContent + "right:");
        sb.append(this.getRightNode().toString(level + 2));

        return sb.toString();
    }

}
