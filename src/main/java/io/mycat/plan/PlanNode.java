package io.mycat.plan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import io.mycat.config.ErrorCode;
import io.mycat.plan.common.context.NameResolutionContext;
import io.mycat.plan.common.context.ReferContext;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;
import io.mycat.plan.common.item.subquery.ItemSubselect;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.TableNode;

public abstract class PlanNode {
	private static final Logger logger = Logger.getLogger(PlanNode.class);
	public enum PlanNodeType {
		NONAME, TABLE, JOIN, MERGE, QUERY, VIEW
	}

	public abstract PlanNodeType type();

	protected String sql;

	private boolean isDistinct = false;

	/**
	 * select查询中的列
	 */
	protected List<Item> columnsSelected = new ArrayList<Item>();

	/**
	 * 显式的由查询接口指定的orderBy，注意需要保证顺序
	 */
	protected List<Order> orderBys = new LinkedList<Order>();

	/**
	 * 显式的由查询接口指定的group by，注意需要保证顺序
	 */
	protected List<Order> groups = new LinkedList<Order>();

	/**
	 * having条件
	 */
	protected Item havingFilter;
	/**
	 * 上一层父节点，比如子查询会依赖父节点的字段信息
	 * http://dev.mysql.com/doc/refman/5.0/en/correlated-subqueries.html
	 */
	private PlanNode parent;

	/**
	 * 子节点
	 */
	protected List<PlanNode> children = new ArrayList<PlanNode>();

	/**
	 * 从哪里开始
	 */
	protected long limitFrom = -1;

	/**
	 * 到哪里结束
	 */
	protected long limitTo = -1;

	/**
	 * filter in where
	 */
	protected Item whereFilter = null;

	/**
	 * 当前tn的别名，用于进行join等操作的时候辨别到底这一行是从哪个subNode来的。
	 */
	protected String alias;

	/**
	 * 如果出现subQuery，内外都存在别名时，内部的别名为subAlias，外部使用的别名为alias
	 * tablenode自身的这个Alias属性和基类TreeNode的alias属性的作用如下： 按照原本tddl的做法无法区分 select *
	 * from (select* from test1 t1) t当中的两个alias
	 * 当tablenode的tableAlias属性有值时，我们就把这个语句带上tableAlias下发下去
	 */
	protected String subAlias;

	/**
	 * 当前节点是否为子查询
	 */
	protected boolean subQuery;

	protected boolean exsitView = false;

	/**
	 * 聚合函数集合
	 */
	public HashSet<ItemSum> sumFuncs = new HashSet<ItemSum>();

	/**
	 * 子查询集合
	 */
	public List<ItemSubselect> subSelects = new ArrayList<ItemSubselect>();

	protected List<TableNode> referedTableNodes = new ArrayList<TableNode>();

	// inner field -> child field
	protected Map<NamedField, NamedField> innerFields = new LinkedHashMap<NamedField, NamedField>();

	protected Map<NamedField, Item> outerFields = new LinkedHashMap<NamedField, Item>();

	protected NameResolutionContext nameContext;

	protected ReferContext referContext;

	protected PlanNode() {
		nameContext = new NameResolutionContext();
		referContext = new ReferContext();
		nameContext.setPlanNode(this);
		referContext.setPlanNode(this);
	}

	/**
	 * 依赖的所有列，保存的是childnode->iselectable
	 */
	protected LoadingCache<PlanNode, List<Item>> columnsReferedCache= CacheBuilder.newBuilder()
			.build(new CacheLoader<PlanNode, List<Item>>() {
				@Override
				public List<Item> load(PlanNode tn) {
					return new ArrayList<Item>();
				}
			});
		

	private List<Item> columnsReferList = new ArrayList<Item>();

	private boolean existUnPushDownGroup = false;

	/**
	 * 是否可全局表优化 ex. TableNode: isGlobaled当且仅当tablename是globaltablename
	 * MergeNode: always false; QueryNode: true <-->subchild is true JoinNode:
	 * 当且仅当所有子节点最多只有一个非globaltable
	 */
	protected Set<String> noshardNode = null;

	// 这个node涉及到的unglobal的表的个数
	protected int unGlobalTableCount = 0;

	protected List<Item> nestLoopFilters = null;
	

	public abstract String getPureName();

	/* 当前节点的高度 */
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
		// where, pushdown node时无须where条件
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
	}

	protected void setUpInnerFields() {
		innerFields.clear();
		String tmpFieldTable;
		String tmpFieldName;
		for (PlanNode child : children) {
			child.setUpFields();
			for (NamedField coutField : child.outerFields.keySet()) {
				tmpFieldTable = child.getAlias() == null ? coutField.getTable() : child.getAlias();
				// view也会有subAlias
				if (subAlias!= null && subAlias.length()!=0)
					tmpFieldTable = subAlias;
				tmpFieldName = coutField.getName();
				NamedField tmpField = new NamedField(tmpFieldTable,tmpFieldName,coutField.planNode);
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
			if (item.type() == ItemType.INT_ITEM) {
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
			if (item.type() == ItemType.INT_ITEM) {
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

	protected void dealStarColumn() {
		List<Item> newSels = new ArrayList<Item>();
		for (Item selItem : columnsSelected) {
			if (selItem.isWild()) {
				ItemField wildField = (ItemField) selItem;
				if (wildField.tableName==null || wildField.tableName.length()==0) {
					for (NamedField field : innerFields.keySet()) {
						ItemField col = new ItemField(null, field.getTable(), field.getName());
						newSels.add(col);
					}
				} else {
					String selTable = wildField.tableName;
					boolean found = false;
					for (NamedField field : innerFields.keySet()) {
						if (selTable != null && selTable.equals(field.getTable())
								|| (selTable == null && field.getTable() == null)) {
							ItemField col = new ItemField(null, field.getTable(), field.getName());
							newSels.add(col);
							found = true;
						} else if (found) {
							// a.* ->a.id,a.id1,b.id 找到b.id时退出
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
		String tmpFieldName =  sel.getItemName();
		if (subAlias != null)
			tmpFieldTable = subAlias;
		if (tmpFieldTable == null)// maybe function
			tmpFieldTable = getPureName();
		if (sel.getAlias() != null)
			tmpFieldName = sel.getAlias();
		NamedField tmpField = new NamedField(tmpFieldTable,tmpFieldName,this);
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

	public PlanNode setLimitFrom(long limitFrom) {
		this.limitFrom = limitFrom;
		return this;
	}

	public long getLimitTo() {
		return limitTo;
	}

	public PlanNode setLimitTo(long limitTo) {
		this.limitTo = limitTo;
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

	public PlanNode query(Item whereFilter) {
		this.whereFilter = whereFilter;
		return this;
	}

	public String getAlias() {
		return alias;
	}

	public PlanNode alias(String alias) {
		this.alias = alias;
		return this;
	}

	public PlanNode subAlias(String alias) {
		this.subAlias = alias;
		return this;
	}

	public List<Order> getGroupBys() {
		return this.groups;
	}

	public PlanNode setGroupBys(List<Order> groups) {
		this.groups = groups;
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

	public PlanNode setColumnsSelected(List<Item> columnsSelected) {
		this.columnsSelected = columnsSelected;
		return this;
	}

	public List<Item> getColumnsReferedByChild(PlanNode tn) {
		try {
			return this.columnsReferedCache.get(tn);
		} catch (ExecutionException e) {
			logger.warn("columnsReferedCache error", e);
		}
		return null;
	}

	public void addSelToReferedMap(PlanNode tn, Item sel) {
		// 使得相同的refermap中的sel具有相同的columnname
		try {
			this.columnsReferedCache.get(tn).add(sel);
		} catch (ExecutionException e) {
			logger.warn("columnsReferedCache error", e);
		}
	}

	public List<Item> getColumnsRefered() {
		return this.columnsReferList;
	}

	/**
	 * 设置别名，表级别
	 */
	public PlanNode setAlias(String string) {
		this.alias(string);
		return this;
	}

	/**
	 * 设置别名，表级别
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

	public PlanNode setSubQuery(boolean subQuery) {
		this.subQuery = subQuery;
		return this;
	}

	public PlanNode having(Item havingFilter) {
		this.havingFilter = havingFilter;
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

	/* 获取改节点下的tablenode集合 */
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
	 * @param exsitView
	 *            the exsitView to set
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
	 * @param isDistinct
	 *            the isDistinct to set
	 */
	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
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
	 * @param nestLoopFilters
	 *            the strategyFilters to set
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
