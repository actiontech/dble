/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.node;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.sharding.table.ERTable;
import com.oceanbase.obsharding_d.plan.NamedField;
import com.oceanbase.obsharding_d.plan.Order;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemField;
import com.oceanbase.obsharding_d.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.oceanbase.obsharding_d.plan.util.FilterUtils;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.plan.util.ToStringUtil;
import com.oceanbase.obsharding_d.route.parser.druid.RouteTableConfigInfo;
import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.util.StringUtil;

import java.util.*;


public class JoinNode extends PlanNode {

    public PlanNodeType type() {
        return PlanNodeType.JOIN;
    }

    public enum Strategy {
        SORTMERGE, NESTLOOP, HINT_NEST_LOOP, ALWAYS_NEST_LOOP
    }

    private boolean isNotIn = false;

    public boolean isNotIn() {
        return isNotIn;
    }

    public void setNotIn(boolean notIn) {
        this.isNotIn = notIn;
    }

    // left node's order of sort-merge-join
    private final List<Order> leftJoinOnOrders = new ArrayList<>();
    private boolean isLeftOrderMatch = false;
    // right node's order of sort-merge-join
    private final List<Order> rightJoinOnOrders = new ArrayList<>();
    private boolean rightOrderMatch = false;

    private List<String> usingFields;

    private boolean isNatural = false;
    private boolean isExistNatural = false;
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

    private final List<ERTable> erKeys = new ArrayList<>();

    private Strategy strategy = Strategy.SORTMERGE;
    private final int charsetIndex;

    public JoinNode(int charsetIndex) {
        this.leftOuter = false;
        this.rightOuter = false;
        this.needOptimizeJoinOrder = true;
        this.joinFilter = new ArrayList<>();
        this.charsetIndex = charsetIndex;
    }

    public JoinNode(PlanNode left, PlanNode right, int charsetIndex) {
        this(charsetIndex);
        addChild(left);
        addChild(right);
        setKeepFieldSchema(left.isKeepFieldSchema() || right.isKeepFieldSchema());
    }

    @Override
    public RouteTableConfigInfo findFieldSourceFromIndex(int index) throws Exception {
        if (columnsSelected.size() > index) {
            Item sourceColumns = columnsSelected.get(index);
            for (PlanNode pn : this.getChildren()) {
                if ((pn.getAlias() != null && pn.getAlias().equals(sourceColumns.getTableName())) ||
                        (pn.getAlias() == null && pn.getPureName().equals(sourceColumns.getTableName()))) {
                    for (int i = 0; i < pn.columnsSelected.size(); i++) {
                        Item cSelected = pn.columnsSelected.get(i);
                        if (cSelected.getAlias() != null && cSelected.getAlias().equalsIgnoreCase(sourceColumns.getItemName())) {
                            return pn.findFieldSourceFromIndex(i);
                        } else if (cSelected.getAlias() == null && cSelected.getItemName().equalsIgnoreCase(sourceColumns.getItemName())) {
                            return pn.findFieldSourceFromIndex(i);
                        }
                    }
                }
            }
        }
        return null;
    }


    @Override
    public void setUpFields() {
        super.setUpFields();
        buildJoinFilters();
        buildOtherJoinOn();
        buildJoinColumns(false);
    }

    @Override
    protected void setUpInnerFields() {
        super.setUpInnerFields();
        if (this.isNatural) {
            this.genUsingByNatural();
        }

    }

    private void buildJoinFilters() {
        nameContext.setFindInSelect(false);
        nameContext.setSelectFirst(false);

        if (usingFields != null) {
            for (String using : usingFields) {
                using = StringUtil.removeBackQuote(using);
                Pair<String, String> lName = findTbNameByUsing(this.getLeftNode(), using);
                Pair<String, String> rName = findTbNameByUsing(this.getRightNode(), using);
                if (lName.equals(rName)) {
                    throw new MySQLOutPutException(ErrorCode.ER_NONUNIQ_TABLE, "42000", "Not unique table/alias: '" + lName.getValue() + "'");
                }
                Item filter = setUpItem(genJoinFilter(using, lName, rName));
                joinFilter.add((ItemFuncEqual) filter);
            }
        } else {
            for (int index = 0; index < joinFilter.size(); index++) {
                Item bf = joinFilter.get(index);
                bf = setUpItem(bf);
                if (bf.getReferTables().size() == 1) {
                    if (bf.getReferTables().iterator().next().type() == PlanNodeType.TABLE) {
                        throw new MySQLOutPutException(ErrorCode.ER_NONUNIQ_TABLE, "42000", "Not unique table/alias: '" + this.getLeftNode().getPureName() + "'");
                    } else {
                        //todo: query node and can push
                        throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "42000", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '" + bf + "'");
                    }
                }
                joinFilter.set(index, (ItemFuncEqual) bf);
            }
        }
    }

    private void genUsingByNatural() {
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
                continue;
            }
            checkDup.add(fieldName);
            fields.add(fieldName);
        }
        return fields;
    }

    private Pair<String, String> findTbNameByUsing(PlanNode node, String using) {
        String schema = null;
        if (node.type() == PlanNodeType.TABLE) {
            schema = ((TableNode) node).getSchema();
        }
        String table = node.getCombinedName();
        if (table != null) {
            return new Pair<>(schema, table);
        }
        for (NamedField field : node.getOuterFields().keySet()) {
            if (field.getName().equalsIgnoreCase(using)) {
                table = field.getTable();
            }
        }
        return new Pair<>(schema, table);
    }

    private ItemFuncEqual genJoinFilter(String using, Pair<String, String> leftJoinNode, Pair<String, String> rightJoinNode) {
        ItemField column1 = new ItemField(leftJoinNode.getKey(), leftJoinNode.getValue(), using);
        ItemField column2 = new ItemField(rightJoinNode.getKey(), rightJoinNode.getValue(), using);
        return new ItemFuncEqual(column1, column2, charsetIndex);
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

    public void setNatural(boolean natural) {
        isNatural = natural;
    }


    public int getCharsetIndex() {
        return charsetIndex;
    }

    @Override
    protected void dealSingleStarColumn(List<Item> newSels) {
        if (isNatural || isExistNatural) {
            dealSingleStarColumnByNatural(newSels);
        } else {
            if (usingFields == null) {
                super.dealSingleStarColumn(newSels);
            } else {
                PlanNode driverNode = this.isRightOuterJoin() ? this.getRightNode() : this.getLeftNode();
                Pair<String, String> tableInfo = findTbNameByUsing(driverNode, usingFields.get(0));
                for (String fieldName : usingFields) {
                    ItemField col = new ItemField(tableInfo.getKey(), tableInfo.getValue(), fieldName);
                    newSels.add(col);
                }

                for (NamedField field : innerFields.keySet()) {
                    if (usingFields.contains(field.getName())) {
                        continue;
                    }
                    ItemField col = new ItemField(field.getSchema(), field.getTable(), field.getName());
                    newSels.add(col);
                }
            }
        }
    }

    /**
     * https://dev.mysql.com/doc/refman/8.0/en/join.html
     * First, coalesced common columns of the two joined tables, in the order in which they occur in the first table
     * Second, columns unique to the first table, in order in which they occur in that table
     * Third, columns unique to the second table, in order in which they occur in that table
     */
    private void dealSingleStarColumnByNatural(List<Item> newSels) {
        boolean parentIsJoin = this.getParent() instanceof JoinNode;
        if (parentIsJoin) {
            ((JoinNode) this.getParent()).isExistNatural = true;
        }
        if (usingFields == null) {
            for (NamedField field : innerFields.keySet()) {
                if (!parentIsJoin) {
                    if (field.isNatualShow()) {
                        ItemField col = new ItemField(field.getSchema(), field.getTable(), field.getName(), field.getCharsetIndex());
                        newSels.add(col);
                    }
                } else {
                    ItemField col = new ItemField(field.getSchema(), field.getTable(), field.getName(), field.getCharsetIndex());
                    if (!field.isNatualShow())
                        col.noNatualShow();
                    newSels.add(col);
                }
            }
            return;
        } else {
            dealSingleStarColumnByNaturalByUsingFields(parentIsJoin, newSels);
        }
    }

    private void dealSingleStarColumnByNaturalByUsingFields(boolean parentIsJoin, List<Item> newSels) {
        List<Item> newSelsTmp = new ArrayList<>();
        List<Item> usingFieldsTmp = new ArrayList<>();

        PlanNode driverNode = this.isRightOuterJoin() ? this.getRightNode() : this.getLeftNode();
        // First
        for (String fieldName : usingFields) {
            for (NamedField field : driverNode.getInnerFields().keySet()) {
                String name = field.getName();
                if (name.equals(fieldName)) {
                    ItemField col = new ItemField(field.getSchema(), field.getTable(), field.getName());
                    usingFieldsTmp.add(col);
                    break;
                }
            }
        }

        // Second
        newSelsTmp.addAll(usingFieldsTmp);
        for (NamedField field : driverNode.getInnerFields().keySet()) {
            ItemField col = new ItemField(field.getSchema(), field.getTable(), field.getName());
            if (usingFieldsTmp.contains(col)) {
                continue;
            }
            if (!field.isNatualShow())
                col.noNatualShow();
            newSelsTmp.add(col);
        }
        // Third (add Remaining innerFields)
        List<Item> naturalColumnsSelected = new LinkedList<>(newSelsTmp);
        for (NamedField field : innerFields.keySet()) {
            ItemField col = new ItemField(field.getSchema(), field.getTable(), field.getName());
            if (!newSelsTmp.contains(col)) {
                newSelsTmp.add(col);
                boolean contians = false;
                for (Item f : naturalColumnsSelected) {
                    if (f.getItemName().equals(field.getName())) {
                        contians = true;
                        break;
                    }
                }
                if (!contians) {
                    naturalColumnsSelected.add(col);
                } else {
                    col.noNatualShow();
                    field.noShow();
                    //innerFields.get(field).noShow();
                }
            }
        }
        if (!parentIsJoin) {
            for (NamedField field : innerFields.keySet()) {
                if (!field.isNatualShow()) {
                    ItemField col = new ItemField(field.getSchema(), field.getTable(), field.getName());
                    newSelsTmp.remove(col);
                }
            }
        }
        newSels.addAll(newSelsTmp);
    }

    /**
     * setupJoinfilters
     *
     * @param clearName if true:clear filter's itemname,else keep
     */
    private void buildJoinColumns(boolean clearName) {
        Iterator<ItemFuncEqual> iterator = joinFilter.iterator();
        while (iterator.hasNext()) {
            ItemFuncEqual bf = iterator.next();
            if (clearName)
                bf.setItemName(null);
            boolean isJoinColumn = PlanUtil.isJoinColumn(bf, this);
            if (!isJoinColumn) {
                otherJoinOnFilter = FilterUtils.and(otherJoinOnFilter, bf);
                iterator.remove();
            }
        }
    }

    public List<Item> getLeftKeys() {
        List<Item> leftKeys = new ArrayList<>(this.getJoinFilter().size());
        for (ItemFuncEqual f : this.getJoinFilter()) {
            leftKeys.add(f.arguments().get(0));
        }
        return leftKeys;
    }

    public List<Item> getRightKeys() {
        List<Item> rightKeys = new ArrayList<>(this.getJoinFilter().size());
        for (ItemFuncEqual f : this.getJoinFilter()) {
            rightKeys.add(f.arguments().get(1));
        }
        return rightKeys;
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
            //todo: needed?
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

    @Override
    public String getPureSchema() {
        return null;
    }

    /**
     * exchangeLeftAndRight
     */
    public void exchangeLeftAndRight() {

        PlanNode tmp = this.getLeftNode();
        this.setLeftNode(this.getRightNode());
        this.setRightNode(tmp);

        boolean tmpOuter = this.leftOuter;
        this.leftOuter = this.rightOuter;
        this.rightOuter = tmpOuter;

        this.buildJoinColumns(true);

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

    public void setRightOuterJoin() {
        this.rightOuter = true;
        this.leftOuter = false;
    }

    public void setInnerJoin() {
        this.leftOuter = false;
        this.rightOuter = false;
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


    private boolean isNeedOptimizeJoinOrder() {
        return this.needOptimizeJoinOrder;
    }

    private void setNeedOptimizeJoinOrder(boolean needOptimizeJoinOrder) {
        this.needOptimizeJoinOrder = needOptimizeJoinOrder;
    }

    public List<Order> getLeftJoinOnOrders() {
        return leftJoinOnOrders;
    }

    public List<Order> getRightJoinOnOrders() {
        return rightJoinOnOrders;
    }

    @Override
    public JoinNode copy() {
        JoinNode newJoinNode = new JoinNode(this.charsetIndex);
        this.copySelfTo(newJoinNode);
        newJoinNode.setJoinFilter(new ArrayList<>());
        for (Item bf : joinFilter) {
            newJoinNode.addJoinFilter((ItemFuncEqual) bf.cloneStruct());
        }
        newJoinNode.setLeftNode(this.getLeftNode().copy());
        newJoinNode.setRightNode(this.getRightNode().copy());
        newJoinNode.setNeedOptimizeJoinOrder(this.isNeedOptimizeJoinOrder());
        newJoinNode.leftOuter = this.leftOuter;
        newJoinNode.rightOuter = this.rightOuter;
        newJoinNode.isNotIn = this.isNotIn;
        newJoinNode.otherJoinOnFilter = this.otherJoinOnFilter == null ? null : this.otherJoinOnFilter.cloneStruct();
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
     * @param leftOrderMatch the isLeftOrderMatch to set
     */
    public void setLeftOrderMatch(boolean leftOrderMatch) {
        this.isLeftOrderMatch = leftOrderMatch;
    }

    /**
     * @return the isRightOrderMatch
     */
    public boolean isRightOrderMatch() {
        return rightOrderMatch;
    }

    /**
     * @param rightOrderMatch the isRightOrderMatch to set
     */
    public void setRightOrderMatch(boolean rightOrderMatch) {
        this.rightOrderMatch = rightOrderMatch;
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
