/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.context.NameResolutionContext;
import com.actiontech.dble.plan.common.context.ReferContext;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.plan.common.item.subquery.ItemSubselect;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.TableNode;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;

public abstract class PlanNode {
    private static final Logger LOGGER = Logger.getLogger(PlanNode.class);

    /**
     * aggregate function list
     */
    public HashSet<ItemSum> getSumFuncs() {
        return sumFuncs;
    }

    public void setSumFuncs(HashSet<ItemSum> sumFuncs) {
        this.sumFuncs = sumFuncs;
    }

    /**
     * subQuery list
     */
    public List<ItemSubselect> getSubSelects() {
        return subSelects;
    }

    public void setSubSelects(List<ItemSubselect> subSelects) {
        this.subSelects = subSelects;
    }

    public enum PlanNodeType {
        NONAME, TABLE, JOIN, MERGE, QUERY, VIEW
    }

    public abstract PlanNodeType type();

    protected String sql;

    private boolean isDistinct = false;

    /**
     * select columns
     */
    protected List<Item> columnsSelected = new ArrayList<>();

    /**
     * orderBy,Notice:field order
     */
    protected List<Order> orderBys = new LinkedList<>();

    /**
     * group By,Notice:field order
     */
    protected List<Order> groups = new LinkedList<>();

    /**
     * having condition
     */
    protected Item havingFilter;
    /**
     * parent node ,eg:subquery may use parent's info
     * TODO:NEED?
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
    protected long limitFrom = -1;

    /**
     * LIMIT TO
     */
    protected long limitTo = -1;

    /**
     * filter in where
     */
    protected Item whereFilter = null;

    /**
     * alias for table node ,used for named subNode when join.
     */
    protected String alias;

    /**
     * subQuery's inside and outside all have alias,subAlias for inside,alias for outside
     * eg: select * from (select* from test1 t1) t
     */
    protected String subAlias;

    /**
     * is this node is subQuery
     */
    protected boolean subQuery;

    protected boolean exsitView = false;

    private HashSet<ItemSum> sumFuncs = new HashSet<>();

    private List<ItemSubselect> subSelects = new ArrayList<>();

    protected List<TableNode> referedTableNodes = new ArrayList<>();

    // inner field -> child field
    protected Map<NamedField, NamedField> innerFields = new LinkedHashMap<>();

    protected Map<NamedField, Item> outerFields = new LinkedHashMap<>();

    protected NameResolutionContext nameContext;

    protected ReferContext referContext;

    protected PlanNode() {
        nameContext = new NameResolutionContext();
        referContext = new ReferContext();
        nameContext.setPlanNode(this);
        referContext.setPlanNode(this);
    }

    /**
     * map :childnode->iselectable
     */
    protected LoadingCache<PlanNode, List<Item>> columnsReferedCache = CacheBuilder.newBuilder().build(
            new CacheLoader<PlanNode, List<Item>>() {
                @Override
                public List<Item> load(PlanNode tn) {
                    return new ArrayList<>();
                }
            });


    private List<Item> columnsReferList = new ArrayList<>();

    private boolean existUnPushDownGroup = false;

    /**
     * is global table?
     * ex. TableNode: isGlobaled when tablename is global tablename
     * MergeNode: always false;
     * QueryNode: true <-->subchild is true
     * JoinNode: all children has only one unglobal table at most
     */
    protected Set<String> noshardNode = null;

    // unGlobalTableCount for this node(contains children)
    protected int unGlobalTableCount = 0;

    protected List<Item> nestLoopFilters = null;

    public abstract String getPureName();

    /* height of node */
    public abstract int getHeight();

    public final String getCombinedName() {
        if (this.getAlias() != null) {
            return this.getAlias();
        }
        if (this.getSubAlias() != null) {
            return this.getSubAlias();
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
        childNode.setParent(this);
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

    public void setUpFields() {
        sumFuncs.clear();
        setUpInnerFields();
        setUpSelects();
        setUpWhere();
        setUpGroupBy();
        setUpHaving();
        setUpOrderBy();
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
                setUpItemRefer(jn.getOtherJoinOnFilter());
            }
        }
        // where, pushdown node does 't need where
        if (!isPushDownNode) {
            setUpItemRefer(whereFilter);
        }
        // group by
        for (Order groupBy : groups) {
            setUpItemRefer(groupBy.getItem());
        }
        // having
        setUpItemRefer(havingFilter);
        // order by
        for (Order orderBy : orderBys) {
            setUpItemRefer(orderBy.getItem());
        }
        // make list
        for (List<Item> selSet : columnsReferedCache.asMap().values()) {
            columnsReferList.addAll(selSet);
        }
    }

    // ==================== helper method =================

    public abstract PlanNode copy();

    protected final void copySelfTo(PlanNode to) {
        to.setAlias(this.alias);
        to.setSubAlias(this.subAlias);
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
        to.setSubQuery(subQuery);
        to.setUnGlobalTableCount(unGlobalTableCount);
        to.setNoshardNode(noshardNode);
    }

    protected void setUpInnerFields() {
        innerFields.clear();
        String tmpFieldTable;
        String tmpFieldName;
        for (PlanNode child : children) {
            child.setUpFields();
            for (NamedField coutField : child.outerFields.keySet()) {
                tmpFieldTable = child.getAlias() == null ? coutField.getTable() : child.getAlias();
                // view may has subAlias
                if (subAlias != null && subAlias.length() != 0)
                    tmpFieldTable = subAlias;
                tmpFieldName = coutField.getName();
                NamedField tmpField = new NamedField(tmpFieldTable, tmpFieldName, coutField.planNode);
                if (innerFields.containsKey(tmpField) && getParent() != null)
                    throw new MySQLOutPutException(ErrorCode.ER_DUP_FIELDNAME, "42S21", "Duplicate column name '" + tmpFieldName + "'");
                innerFields.put(tmpField, coutField);
            }
        }
        if (type() == PlanNodeType.JOIN) {
            JoinNode jn = (JoinNode) this;
            if (jn.isNatural()) {
                jn.genUsingByNatural();
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
        for (Item sel : columnsSelected) {
            setUpItem(sel);
            NamedField field = makeOutNamedField(sel);
            if (outerFields.containsKey(field) && getParent() != null)
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "duplicate field");
            outerFields.put(field, sel);
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
            ItemField col = new ItemField(null, field.getTable(), field.getName());
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
                            ItemField col = new ItemField(null, field.getTable(), field.getName());
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
        String tmpFieldTable = sel.getTableName();
        String tmpFieldName = sel.getItemName();
        if (subAlias != null)
            tmpFieldTable = subAlias;
        if (tmpFieldTable == null)// maybe function
            tmpFieldTable = getPureName();
        if (sel.getAlias() != null)
            tmpFieldName = sel.getAlias();
        NamedField tmpField = new NamedField(tmpFieldTable, tmpFieldName, this);
        return tmpField;
    }

    protected Item setUpItem(Item sel) {
        if (sel == null)
            return null;
        return sel.fixFields(nameContext);
    }

    private void setUpItemRefer(Item sel) {
        if (sel != null)
            sel.fixRefer(referContext);
    }

    // --------------------------getter&setter---------------------------

    public long getLimitFrom() {
        return limitFrom;
    }

    public PlanNode setLimitFrom(long from) {
        this.limitFrom = from;
        return this;
    }

    public long getLimitTo() {
        return limitTo;
    }

    public PlanNode setLimitTo(long to) {
        this.limitTo = to;
        return this;
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

    public PlanNode alias(String aliasName) {
        this.alias = aliasName;
        return this;
    }

    public PlanNode subAlias(String subAliasName) {
        this.subAlias = subAliasName;
        return this;
    }

    public List<Order> getGroupBys() {
        return this.groups;
    }

    public PlanNode setGroupBys(List<Order> groupBys) {
        this.groups = groupBys;
        return this;
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

    public PlanNode setColumnsSelected(List<Item> columns) {
        this.columnsSelected = columns;
        return this;
    }

    public List<Item> getColumnsReferedByChild(PlanNode tn) {
        try {
            return this.columnsReferedCache.get(tn);
        } catch (ExecutionException e) {
            LOGGER.warn("columnsReferedCache error", e);
        }
        return null;
    }

    public void addSelToReferedMap(PlanNode tn, Item sel) {
        // the same ReferedMap's selects have the same columnname
        try {
            this.columnsReferedCache.get(tn).add(sel);
        } catch (ExecutionException e) {
            LOGGER.warn("columnsReferedCache error", e);
        }
    }

    public List<Item> getColumnsRefered() {
        return this.columnsReferList;
    }

    /**
     * setAlias for table
     */
    public PlanNode setAlias(String string) {
        this.alias(string);
        return this;
    }

    /**
     * setSubAlias for table
     */
    public PlanNode setSubAlias(String string) {
        this.subAlias(string);
        return this;
    }

    public String getSubAlias() {
        return subAlias;
    }

    public boolean isSubQuery() {
        return subQuery;
    }

    public PlanNode setSubQuery(boolean isSubQuery) {
        this.subQuery = isSubQuery;
        return this;
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
            parent.exsitView |= this.exsitView;
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
    public boolean isExsitView() {
        return exsitView;
    }

    /**
     * @param exsitView the exsitView to set
     */
    public void setExsitView(boolean exsitView) {
        this.exsitView = exsitView;
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
     * show visualable plan in tree
     *
     * @param level
     * @return
     */
    public abstract String toString(int level);
}
