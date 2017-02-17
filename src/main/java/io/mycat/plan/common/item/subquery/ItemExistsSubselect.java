/**
 * 
 */
package io.mycat.plan.common.item.subquery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import io.mycat.config.ErrorCode;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;

public class ItemExistsSubselect extends ItemSubselect {
	private boolean isNot;

	/**
	 * @param currentDb
	 * @param query
	 */
	public ItemExistsSubselect(String currentDb, SQLSelectQuery query, boolean isNot) {
		super(currentDb, query);
		this.isNot = isNot;
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support!");
	}

	@Override
	public void fixLengthAndDec() {
		// TODO Auto-generated method stub

	}

	@Override
	public BigDecimal valReal() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigInteger valInt() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String valStr() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal valDecimal() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public SQLExpr toExpression() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		// TODO Auto-generated method stub
		return null;
	}

}
