package io.mycat.plan.common.item;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.num.ItemNum;
import io.mycat.plan.common.time.MySQLTime;

public class ItemFloat extends ItemNum {
	private BigDecimal value;

	public ItemFloat(BigDecimal value) {
		this.value = value;
		fixed = true;
		decimals = this.value.scale() > 0 ? this.value.scale() : 0;
	}

	@Override
	public ItemType type() {
		return ItemType.REAL_ITEM;
	}

	@Override
	public FieldTypes fieldType() {
		return FieldTypes.MYSQL_TYPE_DOUBLE;
	}

	@Override
	public BigDecimal valReal() {
		return value;
	}

	@Override
	public BigInteger valInt() {
		return value.toBigInteger();
	}

	@Override
	public BigDecimal valDecimal() {
		return value;
	}

	@Override
	public String valStr() {
		return value.toString();
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		return getDateFromReal(ltime, fuzzydate);
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		return getTimeFromReal(ltime);
	}

	@Override
	public ItemNum neg() {
		value = value.negate();
		return this;
	}

	@Override
	public int hashCode() {
		if (value == null)
			return 0;
		else
			return value.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ItemFloat))
			return false;
		ItemFloat other = (ItemFloat) obj;
		if (value == null || other.value == null)
			return value == null && other.value == null;
		else
			return value.equals(other.value);
	}

	@Override
	public SQLExpr toExpression() {
		return new SQLNumberExpr(value);
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		return new ItemFloat(value);
	}
}
