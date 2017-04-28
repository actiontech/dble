/**
 * 
 */
package io.mycat.plan.common.item.subquery;

import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import io.mycat.config.ErrorCode;
import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.common.context.NameResolutionContext;
import io.mycat.plan.common.context.ReferContext;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.ItemResultField;
import io.mycat.plan.visitor.MySQLPlanNodeVisitor;

public abstract class ItemSubselect extends ItemResultField {
	protected SQLSelectQuery query;
	private String currentDb;
	private PlanNode planNode;

	public enum subSelectType {
		UNKNOWN_SUBS, SINGLEROW_SUBS, EXISTS_SUBS, IN_SUBS, ALL_SUBS, ANY_SUBS
	};

	public subSelectType substype() {
		return subSelectType.UNKNOWN_SUBS;
	}

	public ItemSubselect(String currentDb, SQLSelectQuery query) {
		this.query = query;
		this.currentDb = currentDb;
		init();
	}

	@Override
	public ItemType type() {
		return ItemType.SUBSELECT_ITEM;
	}

	private void init() {
		MySQLPlanNodeVisitor pv = new MySQLPlanNodeVisitor(currentDb);
		pv.visit(this.query);
		this.planNode = pv.getTableNode();
		if (planNode.type() != PlanNodeType.NONAME) {
			this.withSubQuery = true;
		}
	}

	public void reset() {
		this.nullValue = true;
	}

	@Override
	public final boolean isNull() {
		updateNullValue();
		return nullValue;
	}

	@Override
	public boolean fixFields() {
		throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "not supported!");
	}

	public Item fixFields(NameResolutionContext context) {
		this.planNode.setUpFields();
		return this;
	}

	/**
	 * added to construct all refers in an item
	 * 
	 * @param context
	 */
	public void fixRefer(ReferContext context) {
		if (context.isPushDownNode())
			return;
		else
			context.getPlanNode().subSelects.add(this);
	}

	@Override
	public String funcName() {
		return "subselect";
	}

	/**
	 * 计算子查询相关数据
	 */
	public boolean execute() {
		// TODO
		return false;
	}

	public PlanNode getPlanNode() {
		return planNode;
	}

	public void setPlanNode(PlanNode planNode) {
		this.planNode = planNode;
	}

	@Override
	public String toString(){
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported!");
	}
}
