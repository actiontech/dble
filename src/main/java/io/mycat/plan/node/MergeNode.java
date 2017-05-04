package io.mycat.plan.node;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.mycat.config.ErrorCode;
import io.mycat.plan.NamedField;
import io.mycat.plan.PlanNode;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.util.ToStringUtil;

/**
 * @author ActionTech
 * @createTime 2014-1-21
 */
public class MergeNode extends PlanNode {

	private List<Item> comeInFields;

	public PlanNodeType type() {
		return PlanNodeType.MERGE;
	}

	private boolean union = false;

	public MergeNode() {
	}

	public MergeNode merge(PlanNode o) {
		this.addChild(o);
		return this;
	}

	// 记录union字段名称以及对应index的map
	public Map<String, Integer> getColIndexs() {
		Map<String, Integer> colIndexs = new HashMap<String, Integer>();
		for (int index = 0; index < getColumnsSelected().size(); index++) {
			String name = getColumnsSelected().get(index).getItemName();
			colIndexs.put(name, index);
		}
		return colIndexs;
	}

	public String getPureName() {
		return null;
	}

	public boolean isUnion() {
		return union;
	}

	public void setUnion(boolean union) {
		this.union = union;
	}

	public List<Item> getComeInFields() {
		// modify：允许为了让union的order
		// by直接下发，在SelectPush后允许union的select列和child的select列不同，
		// uionhandler的列以firstchild为准，union sql的结果仍然为node.getcolumnSelected
		if (comeInFields == null)
			return getColumnsSelected();
		else
			return comeInFields;
	}

	@Override
	protected void setUpInnerFields() {
		innerFields.clear();
		boolean isFirst = true;
		int columnSize = 0;
		for (PlanNode child : children) {
			child.setUpFields();
			int childSelects = child.getColumnsSelected().size();
			if (isFirst) {
				columnSize = childSelects;
				isFirst = false;
			} else {
				if (columnSize != childSelects) {
					throw new MySQLOutPutException(ErrorCode.ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, "21000",
							"The used SELECT statements have a different number of columns");
				}
			}
		}
	}

	@Override
	protected void setUpSelects() {
		columnsSelected.clear();
		PlanNode firstNode = getChild();
		outerFields.clear();
		Set<NamedField> checkDup = new HashSet<NamedField>(firstNode.getOuterFields().size() , 1);
		for (NamedField coutField : firstNode.getOuterFields().keySet()) {
			ItemField column = new ItemField(null, coutField.table, coutField.name);
			NamedField tmpField = new NamedField(coutField.table, coutField.name, this);
			NamedField testDupField = new NamedField(null, coutField.name, this);
			if (checkDup.contains(testDupField) && getParent() != null) {
				throw new MySQLOutPutException(ErrorCode.ER_DUP_FIELDNAME, "", "Duplicate column name "+coutField.name);
			}
			checkDup.add(testDupField);
			outerFields.put(tmpField, column);
			getColumnsSelected().add(column);
		}
	}

	@Override
	public MergeNode copy() {
		MergeNode newMergeNode = new MergeNode();
		this.copySelfTo(newMergeNode);
		newMergeNode.setUnion(union);
		for (PlanNode child : children) {
			newMergeNode.addChild((PlanNode) child.copy());
		}
		return newMergeNode;
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

	public void setComeInFields(List<Item> comeInFields) {
		this.comeInFields = comeInFields;
	}

	@Override
	public String toString(int level) {
		StringBuilder sb = new StringBuilder();
		String tabTittle = ToStringUtil.getTab(level);
		String tabContent = ToStringUtil.getTab(level + 1);
		if (this.getAlias() != null) {
			ToStringUtil.appendln(sb, tabTittle + (this.isUnion() ? "Union" : "Union all") + " as " + this.getAlias());
		} else {
			ToStringUtil.appendln(sb, tabTittle + (this.isUnion() ? "Union" : "Union all"));
		}
		ToStringUtil.appendln(sb, tabContent + "columns: " + ToStringUtil.itemListString(columnsSelected));
		ToStringUtil.appendln(sb, tabContent + "where: " + ToStringUtil.itemString(whereFilter));
		ToStringUtil.appendln(sb, tabContent + "orderBy: " + ToStringUtil.orderListString(orderBys));
		if (this.getLimitFrom() >= 0L && this.getLimitTo() > 0L) {
			ToStringUtil.appendln(sb, tabContent + "limitFrom: " + this.getLimitFrom());
			ToStringUtil.appendln(sb, tabContent + "limitTo: " + this.getLimitTo());
		}

		for (PlanNode node : children) {
			sb.append(node.toString(level + 2));
		}

		return sb.toString();
	}

}
