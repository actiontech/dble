/**
 * 
 */
package io.mycat.plan.common.item.subquery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;

public class ItemSinglerowSubselect extends ItemSubselect {
	/* 记录一行的row */
	private List<Item> row;
	/* 记录row item相关的fields值 */
	private List<Field> fields;
	private Item value;
	private boolean no_rows;

	public ItemSinglerowSubselect(String currentDb, SQLSelectQuery query) {
		super(currentDb, query);
	}

	@Override
	public subSelectType substype() {
		return subSelectType.SINGLEROW_SUBS;
	}

	@Override
	public void reset() {
		this.nullValue = true;
		if (value != null)
			value.nullValue = true;
	}

	@Override
	public BigDecimal valReal() {
		if (!no_rows && !execute() && !value.nullValue) {
			nullValue = false;
			return value.valReal();
		} else {
			reset();
			return BigDecimal.ZERO;
		}
	}

	@Override
	public BigInteger valInt() {
		if (!no_rows && !execute() && !value.nullValue) {
			nullValue = false;
			return value.valInt();
		} else {
			reset();
			return BigInteger.ZERO;
		}
	}

	@Override
	public String valStr() {
		if (!no_rows && !execute() && !value.nullValue) {
			nullValue = false;
			return value.valStr();
		} else {
			reset();
			return null;
		}
	}

	@Override
	public BigDecimal valDecimal() {
		if (!no_rows && !execute() && !value.nullValue) {
			nullValue = false;
			return value.valDecimal();
		} else {
			reset();
			return null;
		}
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		if (!no_rows && !execute() && !value.nullValue) {
			nullValue = false;
			return value.getDate(ltime, fuzzydate);
		} else {
			reset();
			return true;
		}
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		if (!no_rows && !execute() && !value.nullValue) {
			nullValue = false;
			return value.getTime(ltime);
		} else {
			reset();
			return true;
		}
	}

	@Override
	public boolean valBool() {
		if (!no_rows && !execute() && !value.nullValue) {
			nullValue = false;
			return value.valBool();
		} else {
			reset();
			return false;
		}
	}

	@Override
	public void fixLengthAndDec() {

	}

	@Override
	public SQLExpr toExpression() {
		// TODO
		return null;
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		// TODO Auto-generated method stub
		return null;
	}

	/*--------------------------------------getter/setter-----------------------------------*/
	public List<Field> getFields() {
		return fields;
	}

	public void setFields(List<Field> fields) {
		this.fields = fields;
	}

}
