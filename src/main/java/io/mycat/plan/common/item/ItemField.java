package io.mycat.plan.common.item;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.node.JoinNode;
import org.apache.commons.lang.StringUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;

import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.config.ErrorCode;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.plan.NamedField;
import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.common.context.NameResolutionContext;
import io.mycat.plan.common.context.ReferContext;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.time.MySQLTime;

public class ItemField extends ItemIdent {

	public Field field;

	/* index如果有值的话，代表这个Item_field真实的下标值，需要在调用val之前调用setField方法 */
	public int index = -1;

	public ItemField(String dbName, String tableName, String fieldName) {
		super(dbName, tableName, fieldName);
	}

	public ItemField(Field field) {
		super(null, field.table, field.name);
		set_field(field);
	}

	/**
	 * 保存index
	 * 
	 * @param index
	 */
	public ItemField(int index) {
		super(null, "", "");
		this.index = index;
	}

	public void setField(List<Field> fields) {
		assert (fields != null);
		set_field(fields.get(index));
	}

	@Override
	public ItemType type() {
		return ItemType.FIELD_ITEM;
	}

	@Override
	public ItemResult resultType() {
		return field.resultType();
	}

	@Override
	public ItemResult numericContextResultType() {
		return field.numericContextResultType();
	}

	@Override
	public FieldTypes fieldType() {
		return field.fieldType();
	}

	@Override
	public byte[] getRowPacketByte() {
		return field.ptr;
	}

	public ItemResult cmp_type() {
		return field.cmpType();
	}

	@Override
	public int hashCode() {
		int prime = 31;
		int hashCode = dbName == null ? 0 : dbName.hashCode();
		hashCode = hashCode * prime + (tableName == null ? 0 : tableName.hashCode());
		hashCode = hashCode * prime + (itemName == null ? 0 : itemName.hashCode());
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ItemField))
			return false;
		ItemField other = (ItemField) obj;
		return StringUtils.equals(getTableName(), other.getTableName())
				&& StringUtils.equalsIgnoreCase(getItemName(), other.getItemName());
	}

	@Override
	public BigDecimal valReal() {
		if (nullValue = field.isNull())
			return BigDecimal.ZERO;
		return field.valReal();
	}

	@Override
	public BigInteger valInt() {
		if (nullValue = field.isNull())
			return BigInteger.ZERO;
		return field.valInt();
	}

	@Override
	public long valTimeTemporal() {
		if ((nullValue = field.isNull()))
			return 0;
		return field.valTimeTemporal();
	}

	@Override
	public long valDateTemporal() {
		if ((nullValue = field.isNull()))
			return 0;
		return field.valDateTemporal();
	}

	@Override
	public BigDecimal valDecimal() {
		if (nullValue = field.isNull())
			return null;
		return field.valDecimal();
	}

	@Override
	public String valStr() {
		if (nullValue = field.isNull())
			return null;
		return field.valStr();
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		if ((nullValue = field.isNull()) || field.getDate(ltime, fuzzydate)) {
			ltime.set_zero_time(ltime.time_type);
			return true;
		}
		return false;
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		if ((nullValue = field.isNull()) || field.getTime(ltime)) {
			ltime.set_zero_time(ltime.time_type);
			return true;
		}
		return false;
	}

	@Override
	public boolean isNull() {
		return field.isNull();
	}

	@Override
	public void makeField(FieldPacket fp) {
		field.makeField(fp);
		try {
			if (itemName != null) {
				fp.name = itemName.getBytes(CharsetUtil.getJavaCharset(charsetIndex));
			}
			if ((tableName != null)) {
				fp.table = tableName.getBytes(CharsetUtil.getJavaCharset(charsetIndex));
			}
			if (dbName != null) {
				fp.db = dbName.getBytes(CharsetUtil.getJavaCharset(charsetIndex));
			}
		} catch (UnsupportedEncodingException e) {
			logger.warn("parse string exception!", e);
		}
	}

	protected void set_field(Field field) {
		this.field = field;
		maybeNull = field.maybeNull(); // 有可能为null
		decimals = field.decimals;
		tableName = field.table;
		itemName = field.name;
		dbName = field.dbname;
		maxLength = field.fieldLength;
		charsetIndex = field.charsetIndex;
		fixed = true;
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public Item fixFields(NameResolutionContext context) {
		if (this.isWild())
			return this;
		NamedField tmpField = new NamedField(null);
		tmpField.name = getItemName();
		PlanNode planNode = context.getPlanNode();
		Item column = null;
		Item columnFromMeta = null;
		if (context.getPlanNode().type() == PlanNodeType.MERGE) {
			// select union only found in outerfields
			if (StringUtils.isEmpty(getTableName())) {
				PlanNode firstNode = planNode.getChild();
				boolean found = false;
				for (NamedField coutField : firstNode.getOuterFields().keySet()) {
					if (coutField.name.equalsIgnoreCase(tmpField.name)) {
						if (!found) {
							tmpField.table = coutField.table;
							found = true;
						} else {
							throw new MySQLOutPutException(ErrorCode.ER_BAD_FIELD_ERROR, "(42S22",
									"Unknown column '" + tmpField.name + "' in 'order clause'");
						}
					}
				}
				column = planNode.getOuterFields().get(tmpField);
			} else {
				throw new MySQLOutPutException(ErrorCode.ER_TABLENAME_NOT_ALLOWED_HERE, "42000",
						"Table '" + getTableName() + "' from one of the SELECTs cannot be used in global ORDER clause");
			}
			return column;
		}
		if (context.isFindInSelect()) {
			// 尝试从selectlist中查找一次
			if (StringUtils.isEmpty(getTableName())) {
				for (NamedField field : planNode.getOuterFields().keySet()) {
					if (StringUtils.equalsIgnoreCase(tmpField.name, field.name)) {
						if (column == null) {
							column = planNode.getOuterFields().get(field);
						} else
							throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "42S22", "duplicate column:" + this);
					}
				}
			} else {
				tmpField.table = getTableName();
				column = planNode.getOuterFields().get(tmpField);
			}
		}
		if (column != null && context.isSelectFirst()) {
			return column;
		}
		// find from inner fields
		if (StringUtils.isEmpty(getTableName())) {
			for (NamedField field : planNode.getInnerFields().keySet()) {
				if (StringUtils.equalsIgnoreCase(tmpField.name, field.name)) {
					if (columnFromMeta == null) {
						tmpField.table = field.table;
						NamedField coutField = planNode.getInnerFields().get(tmpField);
						this.tableName = tmpField.table;
						getReferTables().clear();
						this.getReferTables().add(coutField.planNode);
						columnFromMeta = this;
					} else {
						if (planNode.type() == PlanNodeType.JOIN) {
							JoinNode jn = (JoinNode) planNode;
							if (jn.getUsingFields() != null && jn.getUsingFields().contains(columnFromMeta.getItemName().toLowerCase())) {
								continue;
							}
						}
						throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "42S22", "duplicate column:" + this);
					}
				}
			}
		} else {
			tmpField.table = getTableName();
			if (planNode.getInnerFields().containsKey(tmpField)) {
				NamedField coutField = planNode.getInnerFields().get(tmpField);
				getReferTables().clear();
				getReferTables().add(coutField.planNode);
				this.tableName = tmpField.table;
				columnFromMeta = this;
			}
		}
		if (columnFromMeta != null) {
			return columnFromMeta;
		} else if (column == null)
			throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "42S22", "column " + this + " not found");
		else {
			return column;
		}

	}

	@Override
	public void fixRefer(ReferContext context) {
		if (isWild())
			return;
		PlanNode node = context.getPlanNode();
		PlanNode tn = getReferTables().iterator().next();
		node.addSelToReferedMap(tn, this);
	}

	@Override
	public SQLExpr toExpression() {
		SQLIdentifierExpr parent = tableName == null ? null : new SQLIdentifierExpr(tableName);
		if(parent !=null){
			return new SQLPropertyExpr(parent, itemName);
		}
		else return new SQLIdentifierExpr(itemName);
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		return new ItemField(dbName, tableName, itemName);
	}

}
