/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.node;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.context.NameResolutionContext;
import com.actiontech.dble.plan.common.context.ReferContext;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemFuncGroupConcat;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.plan.common.item.subquery.ItemSubQuery;
import com.actiontech.dble.route.parser.druid.RouteTableConfigInfo;
import com.actiontech.dble.singleton.TraceManager;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.util.*;

public abstract class PlanNode {

    /**
     * select subQuery list
     */
    public List<ItemSubQuery> getSubQueries() {
        return subQueries;
    }

    public enum PlanNodeType {
        NONAME, TABLE, JOIN, MERGE, QUERY, JOIN_INNER, MANAGER_TABLE
    }

    public abstract PlanNodeType type();

    protected String sql;
    protected SQLSelectStatement ast;

    private boolean isDistinct = false;

    /**
     * select columns
     */
    List<Item> columnsSelected = new ArrayList<>();

    /**
     * orderBy,Notice:field order
     */
    List<Order> orderBys = new LinkedList<>();

    /**
     * group By,Notice:field order
     */
    protected List<Order> groups = new LinkedList<>();

    /**
     * having condition
     */
    Item havingFilter;
    /**
     * parent node ,eg:subquery may use parent's info
     * http://dev.mysql.com/doc/refman/5.0/en/correlated-subqueries.html
     */
    private PlanNode parent;

    /**
     * children NODE
     */
    protected List<PlanNode> children = new ArrayList<>();

    /**
     * LIMIT FROM
     */
    private long limitFrom = -1;

    /**
     * LIMIT TO
     */
    private long limitTo = -1;

    /**
     * filter in where
     */
    Item whereFilter = null;

    /**
     * alias for table node ,used for named subNode when join.
     */
    protected String alias;
    /**
     * is this node is subQuery
     */
    private boolean withSubQuery;
    /**
     * is this node contains subQuery
     */
    private boolean containsSubQuery;
    private boolean correlatedSubQuery;

    private boolean existView = false;

    private HashSet<ItemSum> sumFuncs = new HashSet<>();

    private List<ItemSubQuery> subQueries = new ArrayList<>();

    List<TableNode> referedTableNodes = new ArrayList<>();

    // inner field -> child field
    Map<NamedField, NamedField> innerFields = new LinkedHashMap<>();

    Map<NamedField, Item> outerFields = new LinkedHashMap<>();

    NameResolutionContext nameContext;

    private ReferContext referContext;

    protected boolean keepFieldSchema = true;

    protected PlanNode() {
        nameContext = new NameResolutionContext();
        referContext = new ReferContext();
        nameContext.setPlanNode(this);
        referContext.setPlanNode(this);
    }

    /**
     * map :childnode->iselectable
     */
    private Map<PlanNode, List<Item>> columnsReferredCache = new LinkedHashMap<>();


    private List<Item> columnsReferList = new ArrayList<>();

    private boolean existUnPushDownGroup = false;

    /**
     * is global table?
     * ex. TableNode: isGlobaled when tablename is global tablename
     * MergeNode: always false;
     * QueryNode: true <-->subchild is true
     * JoinNode: all children has only one unglobal table at most
     */
    private Set<String> noshardNode = null;

    // unGlobalTableCount for this node(contains children)
    int unGlobalTableCount = 0;

    private List<Item> nestLoopFilters = null;

    public abstract String getPureName();

    public abstract String getPureSchema();

    /* height of node */
    public abstract int getHeight();

    final String getCombinedName() {
        if (this.getAlias() != null) {
            return this.getAlias();
        }
        return this.getPureName();
    }

    public PlanNode getChild() {
        if (children.isEmpty())
            return null;
        else
            return children.get(0);
    }

    public void addChild(PlanNode childNode) {
        if (childNode != null) {
            childNode.setParent(this);
        }
        this.children.add(childNode);
    }

    public PlanNode select(List<Item> columnSelected) {
        this.columnsSelected = columnSelected;
        return this;
    }

    public PlanNode groupBy(Item c, SQLOrderingSpecification sortOrder) {
        Order order = new Order(c, sortOrder);
        this.groups.add(order);
        return this;
    }

    public PlanNode orderBy(Item c, SQLOrderingSpecification sortOrder) {
        if (sortOrder == null) {
            sortOrder = SQLOrderingSpecification.ASC;
        }
        Order order = new Order(c, sortOrder);
        if (!this.orderBys.contains(order)) {
            this.orderBys.add(order);
        }
        return this;
    }

    public abstract RouteTableConfigInfo findFieldSourceFromIndex(int index) throws Exception;

    public void setUpFields() {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("set-up-field-for-sql");
        try {
            sumFuncs.clear();
            setUpInnerFields();
            setUpSelects();
            setUpWhere();
            setUpGroupBy();
            setUpHaving();
            setUpOrderBy();
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    // column refered start
    public void setUpRefers(boolean isPushDownNode) {
        sumFuncs.clear();
        referContext.setPushDownNode(isPushDownNode);
        // select
        for (Item sel : columnsSelected) {
            setUpItemRefer(sel);
        }
        if (type() == PlanNodeType.JOIN) {
            JoinNode jn = (JoinNode) this;
            if (!isPushDownNode) {
                for (Item bf : jn.getJoinFilter())
                    setUpItemRefer(bf);
                setUpFilterRefer(jn.getOtherJoinOnFilter());
            }
        }
        // where, pushdown node does 't need where
        if (!isPushDownNode) {
            setUpFilterRefer(whereFilter);
        }
        // group by
        for (Order groupBy : groups) {
            setUpItemRefer(groupBy.getItem());
        }
        // having
        setUpFilterRefer(havingFilter);
        // order by
        for (Order orderBy : orderBys) {
            setUpItemRefer(orderBy.getItem());
        }
        // make list
        for (List<Item> selSet : columnsReferredCache.values()) {
            columnsReferList.addAll(selSet);
        }
    }

    // ==================== helper method =================

    public abstract PlanNode copy();

    protected final void copySelfTo(PlanNode to) {
        to.setAlias(this.alias);
        to.setDistinct(this.isDistinct);
        for (Item selected : this.getColumnsSelected()) {
            Item copySel = selected.cloneItem();
            copySel.setItemName(selected.getItemName());
            to.columnsSelected.add(copySel);
        }
        for (Order groupBy : this.getGroupBys()) {
            to.groups.add(groupBy.copy());
        }
        for (Order orderBy : this.getOrderBys()) {
            to.orderBys.add(orderBy.copy());
        }
        to.whereFilter = this.whereFilter == null ? null : this.whereFilter.cloneItem();
        to.havingFilter = this.havingFilter == null ? null : havingFilter.cloneItem();
        to.setLimitFrom(this.limitFrom);
        to.setLimitTo(this.limitTo);
        to.setSql(this.getSql());
        to.setWithSubQuery(withSubQuery);
        to.setContainsSubQuery(containsSubQuery);
        to.setCorrelatedSubQuery(correlatedSubQuery);
        to.setUnGlobalTableCount(unGlobalTableCount);
        to.setNoshardNode(noshardNode);
        to.getSubQueries().addAll(subQueries);
        to.setKeepFieldSchema(keepFieldSchema);
    }

    protected void setUpInnerFields() {
        innerFields.clear();
        for (PlanNode child : children) {
            child.setUpFields();
            for (NamedField coutField : child.outerFields.keySet()) {
                String tmpFieldSchema = null;
                if (child.isKeepFieldSchema()) {
                    tmpFieldSchema = child.type() == PlanNodeType.TABLE ? ((TableNode) child).getSchema() : coutField.getSchema();
                }
                String tmpFieldTable = child.getAlias() == null ? coutField.getTable() : child.getAlias();
                String tmpFieldName = coutField.getName();
                NamedField tmpField = new NamedField(tmpFieldSchema, tmpFieldTable, tmpFieldName, coutField.planNode);
                if (innerFields.containsKey(tmpField) && getParent() != null)
                    throw new MySQLOutPutException(ErrorCode.ER_DUP_FIELDNAME, "42S21", "Duplicate column name '" + tmpFieldName + "'");
                innerFields.put(tmpField, coutField);
            }
        }
    }

    protected void setUpSelects() {
        if (columnsSelected.isEmpty()) {
            columnsSelected.add(new ItemField(null, null, "*"));
        }
        boolean withWild = false;
        for (Item sel : columnsSelected) {
            if (sel.isWild())
                withWild = true;
        }
        if (withWild)
            dealStarColumn();
        outerFields.clear();
        nameContext.setFindInSelect(false);
        nameContext.setSelectFirst(false);
        if (parent instanceof MergeNode) {
            if (parent.getChildren().get(1) != null && parent.getChildren().get(1) == this) {
                List<Item> aliasList = parent.getChildren().get(0).getColumnsSelected();
                if (aliasList.size() != columnsSelected.size()) {
                    throw new MySQLOutPutException(ErrorCode.ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, "21000",
                            "The used SELECT statements have a different number of columns");
                }
                for (int i = 0; i < columnsSelected.size(); i++) {
                    Item sel = columnsSelected.get(i);
                    Item beforeUnion = aliasList.get(i);
                    sel.setAlias(beforeUnion.getAlias() == null ? beforeUnion.getItemName() : beforeUnion.getAlias());
                }
            }
        }

        for (Item sel : columnsSelected) {
            setUpItem(sel);
            if (sel instanceof ItemFuncGroupConcat) {
                ((ItemFuncGroupConcat) sel).fixOrders(nameContext);
            }
            NamedField field = makeOutNamedField(sel);
            if (outerFields.containsKey(field) && isDuplicateField(this))
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "duplicate field");
            outerFields.put(field, sel);
        }
    }

    protected boolean isDuplicateField(PlanNode node) {
        if (node.parent == null) {
            return false;
        } else if (node.parent.type() == PlanNodeType.MERGE) {
            return isDuplicateField(node.parent);
        } else {
            return true;
        }
    }

    private void setUpWhere() {
        nameContext.setFindInSelect(false);
        nameContext.setSelectFirst(false);
        whereFilter = setUpItem(whereFilter);
    }

    private void setUpGroupBy() {
        nameContext.setFindInSelect(true);
        nameContext.setSelectFirst(false);
        for (Order order : groups) {
            Item item = order.getItem();
            if (item.type() == Item.ItemType.INT_ITEM) {
                int index = item.valInt().intValue();
                if (index >= 1 && index <= getColumnsSelected().size())
                    order.setItem(getColumnsSelected().get(index - 1));
                else
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                            "Unknown column '" + index + "' in group statement");
            } else {
                order.setItem(setUpItem(item));
            }
        }
    }

    private void setUpHaving() {
        nameContext.setFindInSelect(true);
        nameContext.setSelectFirst(true);
        havingFilter = setUpItem(havingFilter);
    }

    private void setUpOrderBy() {
        nameContext.setFindInSelect(true);
        nameContext.setSelectFirst(true);
        for (Order order : orderBys) {
            Item item = order.getItem();
            if (item.type() == Item.ItemType.INT_ITEM) {
                int index = item.valInt().intValue();
                if (index >= 1 && index <= getColumnsSelected().size())
                    order.setItem(getColumnsSelected().get(index - 1));
                else
                    throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                            "Unknown column '" + index + "' in order statement");
            } else {
                order.setItem(setUpItem(item));
            }
        }
    }

    protected void dealSingleStarColumn(List<Item> newSels) {
        for (NamedField field : innerFields.keySet()) {
            ItemField col = new ItemField(field.getSchema(), field.getTable(), field.getName());
            newSels.add(col);
        }
    }

    protected void dealStarColumn() {
        List<Item> newSels = new ArrayList<>();
        for (Item selItem : columnsSelected) {
            if (selItem.isWild()) {
                ItemField wildField = (ItemField) selItem;
                if (wildField.getTableName() == null || wildField.getTableName().length() == 0) {
                    dealSingleStarColumn(newSels);
                } else {
                    String selTable = wildField.getTableName();
                    boolean found = false;
                    for (NamedField field : innerFields.keySet()) {
                        if (selTable.equals(field.getTable())) {
                            ItemField col = new ItemField(field.getSchema(), field.getTable(), field.getName());
                            newSels.add(col);
                            found = true;
                        } else if (found) {
                            // a.* ->a.id,a.id1,b.id ,break when find b.id
                            break;
                        }
                    }
                    if (!found) {
                        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                                "child table " + selTable + " not exist!");
                    }
                }
            } else {
                newSels.add(selItem);
            }
        }
        columnsSelected = newSels;
    }

    private NamedField makeOutNamedField(Item sel) {
        String tmpSchema = null;
        if (keepFieldSchema) {
            tmpSchema = sel.getDbName();
            if (sel.basicConstItem()) {
                tmpSchema = getPureSchema();
            }
        }
        String tmpFieldTable = sel.getTableName();
        String tmpFieldName = sel.getItemName();
        if (alias != null)
            tmpFieldTable = alias;
        if (tmpFieldTable == null)// maybe function
            tmpFieldTable = getPureName();
        if (sel.getAlias() != null)
            tmpFieldName = sel.getAlias();
        return new NamedField(tmpSchema, tmpFieldTable, tmpFieldName, this);
    }

    Item setUpItem(Item sel) {
        if (sel == null)
            return null;
        return sel.fixFields(nameContext);
    }

    private void setUpFilterRefer(Item filter) {
        if (filter != null) {
            if (filter instanceof ItemFunc) {
                filter.setPushDownName(null);
                for (Item item : filter.arguments()) {
                    setUpFilterRefer(item);
                }
            } else {
                setUpItemRefer(filter);
            }
        }
    }

    private void setUpItemRefer(Item sel) {
        if (sel != null)
            sel.fixRefer(referContext);
    }

    // --------------------------getter&setter---------------------------

    public long getLimitFrom() {
        return limitFrom;
    }

    /**
     * aggregate function list
     */
    public HashSet<ItemSum> getSumFuncs() {
        return sumFuncs;
    }

    public void setSumFuncs(HashSet<ItemSum> sumFuncs) {
        this.sumFuncs = sumFuncs;
    }

    public PlanNode setLimitFrom(long from) {
        this.limitFrom = from;
        return this;
    }

    public PlanNode setLimitTo(long to) {
        this.limitTo = to;
        return this;
    }

    public PlanNode setGroupBys(List<Order> groupBys) {
        this.groups = groupBys;
        return this;
    }

    public PlanNode setColumnsSelected(List<Item> columns) {
        this.columnsSelected = columns;
        return this;
    }

    /**
     * @param existView the exsitView to set
     */
    public void setExistView(boolean existView) {
        this.existView = existView;
    }

    public long getLimitTo() {
        return limitTo;
    }

    public PlanNode limit(long i, long j) {
        this.setLimitFrom(i);
        this.setLimitTo(j);
        return this;
    }

    public Item getWhereFilter() {
        return whereFilter;
    }

    public PlanNode query(Item where) {
        this.whereFilter = where;
        return this;
    }

    public String getAlias() {
        return alias;
    }

    public List<Order> getGroupBys() {
        return this.groups;
    }

    public List<Order> getOrderBys() {
        return orderBys;
    }

    public void setOrderBys(List<Order> orderBys) {
        this.orderBys = orderBys;
    }

    public List<PlanNode> getChildren() {
        return this.children;
    }

    public List<Item> getColumnsSelected() {
        return columnsSelected;
    }

    public List<Item> getColumnsReferedByChild(PlanNode tn) {
        return this.columnsReferredCache.get(tn);
    }

    public void addSelToReferedMap(PlanNode tn, Item sel) {
        // the same ReferedMap's selects have the same columnname
        this.columnsReferredCache.computeIfAbsent(tn, k -> new ArrayList<>());
        this.columnsReferredCache.get(tn).add(sel);
    }

    public List<Item> getColumnsRefered() {
        return this.columnsReferList;
    }

    /**
     * setAlias for table
     */
    public PlanNode setAlias(String aliasName) {
        if (aliasName != null && aliasName.charAt(0) == '`') {
            aliasName = aliasName.substring(1, aliasName.length() - 1);
        }
        this.alias = aliasName;
        return this;
    }

    public boolean isWithSubQuery() {
        return withSubQuery;
    }

    public void setWithSubQuery(boolean withSubQuery) {
        this.withSubQuery = withSubQuery;
    }

    public boolean isContainsSubQuery() {
        return containsSubQuery;
    }

    public void setContainsSubQuery(boolean containsSubQuery) {
        this.containsSubQuery = containsSubQuery;
    }


    public boolean isCorrelatedSubQuery() {
        return correlatedSubQuery;
    }

    public void setCorrelatedSubQuery(boolean correlatedSubQuery) {
        this.correlatedSubQuery = correlatedSubQuery;
    }

    public PlanNode having(Item having) {
        this.havingFilter = having;
        return this;
    }

    public Item getHavingFilter() {
        return this.havingFilter;
    }

    public void setWhereFilter(Item whereFilter) {
        this.whereFilter = whereFilter;
    }


    public int getUnGlobalTableCount() {
        return unGlobalTableCount;
    }

    public void setUnGlobalTableCount(int unGlobalTableCount) {
        this.unGlobalTableCount = unGlobalTableCount;
    }

    public Set<String> getNoshardNode() {
        return noshardNode;
    }

    public void setNoshardNode(Set<String> noshardNode) {
        this.noshardNode = noshardNode;
    }

    /* get tablenode list */
    public List<TableNode> getReferedTableNodes() {
        return referedTableNodes;
    }

    public PlanNode getParent() {
        return parent;
    }

    public void setParent(PlanNode parent) {
        this.parent = parent;
        if (parent != null) {
            parent.referedTableNodes.addAll(referedTableNodes);
            parent.existView |= this.existView;
        }
    }

    /**
     * @return the innerFields
     */
    public Map<NamedField, NamedField> getInnerFields() {
        return innerFields;
    }

    /**
     * @return the outerFields
     */
    public Map<NamedField, Item> getOuterFields() {
        return outerFields;
    }

    /**
     * @return the exsitView
     */
    public boolean isExistView() {
        return this.type() == PlanNode.PlanNodeType.QUERY || existView;
    }

    public boolean existUnPushDownGroup() {
        return existUnPushDownGroup;
    }

    public void setExistUnPushDownGroup(boolean existUnPushDownGroup) {
        this.existUnPushDownGroup = existUnPushDownGroup;
    }

    /**
     * @return the isDistinct
     */
    public boolean isDistinct() {
        return isDistinct;
    }

    /**
     * @param distinct the isDistinct to set
     */
    public void setDistinct(boolean distinct) {
        this.isDistinct = distinct;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public SQLSelectStatement getAst() {
        return ast;
    }

    public void setAst(SQLSelectStatement ast) {
        this.ast = ast;
    }

    /**
     * @return the strategyFilters
     */
    public List<Item> getNestLoopFilters() {
        return nestLoopFilters;
    }

    /**
     * @param nestLoopFilters the strategyFilters to set
     */
    public void setNestLoopFilters(List<Item> nestLoopFilters) {
        this.nestLoopFilters = nestLoopFilters;
    }

    @Override
    public final String toString() {
        return this.toString(0);
    }

    /**
     * show plan in tree
     *
     * @param level level
     * @return String
     */
    public abstract String toString(int level);

    public void setKeepFieldSchema(boolean keepFieldSchema) {
        this.keepFieldSchema = keepFieldSchema;
    }

    public boolean isKeepFieldSchema() {
        return keepFieldSchema;
    }

}
