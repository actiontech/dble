/**
 * 
 */
package io.mycat.plan.common.item.subquery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import io.mycat.config.ErrorCode;
import io.mycat.plan.common.context.NameResolutionContext;
import io.mycat.plan.common.context.ReferContext;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;

public class ItemInSubselect extends ItemSubselect {
	private boolean isNeg;
	private Item leftOprand;

	/**
	 * @param currentDb
	 * @param query
	 */
	public ItemInSubselect(String currentDb, Item leftOprand, SQLSelectQuery query, boolean isNeg) {
		super(currentDb, query);
		this.leftOprand = leftOprand;
		this.isNeg = isNeg;
	}

	@Override
	public void fixLengthAndDec() {

	}

	public Item fixFields(NameResolutionContext context) {
		super.fixFields(context);
		leftOprand = leftOprand.fixFields(context);
		return this;
	}

	/**
	 * added to construct all refers in an item
	 * 
	 * @param context
	 */
	public void fixRefer(ReferContext context) {
		super.fixRefer(context);
		leftOprand.fixRefer(context);
	}

	@Override
	public BigDecimal valReal() {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
	}

	@Override
	public BigInteger valInt() {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
	}

	@Override
	public String valStr() {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
	}

	@Override
	public BigDecimal valDecimal() {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
	}

	@Override
	public boolean getDate(MySQLTime ltime, long fuzzydate) {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
	}

	@Override
	public boolean getTime(MySQLTime ltime) {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
	}

	public Item getLeftOprand() {
		return leftOprand;
	}

	public boolean isNeg() {
		return isNeg;
	}

	@Override
	public SQLExpr toExpression() {
		SQLExpr expr = leftOprand.toExpression();
		SQLSelect select = new SQLSelect(query);
		SQLInSubQueryExpr insub = new SQLInSubQueryExpr(select);
		insub.setExpr(expr);
		insub.setNot(isNeg);
		return insub;
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "unexpected!");
	}

}
