package io.mycat.plan.common.item;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.num.ItemNum;
import io.mycat.plan.common.time.MySQLTime;

public class ItemInt extends ItemNum {
	private BigInteger value;

	public ItemInt(long value) {
		this.value = BigInteger.valueOf(value);
		fixed = true;
		maxLength = String.valueOf(value).length();
	}

	public ItemInt(Integer value) {
		this.value = BigInteger.valueOf(value);
		fixed = true;
		maxLength = String.valueOf(value).length();
	}

	@Override
	public ItemType type() {
		return ItemType.INT_ITEM;
	}

	@Override
	public ItemResult resultType() {
		return ItemResult.INT_RESULT;
	}

	@Override
	public FieldTypes fieldType() {
		return FieldTypes.MYSQL_TYPE_LONGLONG;
	}

	@Override
	public BigDecimal valReal() {
		return new BigDecimal(value);
	}

	@Override
	public BigInteger valInt() {
		return value;
	}

	@Override
	public String valStr() {
		return value.toString();
	}

	@Override
	public BigDecimal valDecimal() {
		return valReal();
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		return getDateFromInt(ltime, fuzzydate);
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		return getTimeFromInt(ltime);
	}

	@Override
	public int decimalPrecision() {
		return maxLength - (value.signum() == -1 ? 1 : 0);
	}

	@Override
	public ItemNum neg() {
		this.value = this.value.negate();
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
		ItemInt other = (ItemInt) obj;
		if (value == null || other.value == null)
			return value == null && other.value == null;
		else
			return value.equals(other.value);
	}

	@Override
	public SQLExpr toExpression() {
		return new SQLIntegerExpr(value);
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		return new ItemInt(value.longValue());
	}


}
