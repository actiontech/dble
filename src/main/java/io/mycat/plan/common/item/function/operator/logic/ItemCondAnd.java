package io.mycat.plan.common.item.function.operator.logic;

import java.math.BigInteger;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;


public class ItemCondAnd extends ItemCond {

	public ItemCondAnd(List<Item> args) {
		super(args);
	}
	
	@Override
	public final String funcName() {
		return "and";
	}

	@Override
	public Functype functype() {
		return Functype.COND_AND_FUNC;
	}

	@Override
	public BigInteger valInt() {
		nullValue = false;
		for (Item item : list) {
			if (!item.valBool()) {
				if (abort_on_null || !(nullValue = item.nullValue))
					return BigInteger.ZERO; // return FALSE
			}
		}
		return nullValue ? BigInteger.ZERO : BigInteger.ONE;
	}
	
	@Override
	public SQLExpr toExpression() {
		SQLExpr left = args.get(0).toExpression();
		SQLExpr right = args.get(1).toExpression();
		return new SQLBinaryOpExpr(left, SQLBinaryOperator.BooleanAnd, right);
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		List<Item> newArgs = null;
		if(!forCalculate)
			newArgs = cloneStructList(args);
		else
			newArgs = calArgs;
		return new ItemCondAnd(newArgs);
	}

}
