package io.mycat.plan.common.item.function.operator.cmpfunc;

import java.math.BigInteger;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.primary.ItemBoolFunc;

public class ItemFuncIsnotnull extends ItemBoolFunc {

	public ItemFuncIsnotnull(Item a) {
		super(a);
	}

	@Override
	public final String funcName() {
		return "isnotnull";
	}

	@Override
	public Functype functype() {
		return Functype.ISNOTNULL_FUNC;
	}

	@Override
	public BigInteger valInt() {
		if (args.get(0).isNull()) {
			return BigInteger.ZERO;
		} else {
			return BigInteger.ONE;
		}
	}

	@Override
	public void fixLengthAndDec() {
		decimals = 0;
		maxLength = 1;
		maybeNull = false;
	}

	@Override
	public SQLExpr toExpression() {
		SQLExpr left = args.get(0).toExpression();
		return new SQLBinaryOpExpr(left, SQLBinaryOperator.IsNot, new SQLIdentifierExpr("UNKNOWN"));
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		List<Item> newArgs = null;
		if (!forCalculate)
			newArgs = cloneStructList(args);
		else
			newArgs = calArgs;
		return new ItemFuncIsnotnull(newArgs.get(0));
	}

}
