package io.mycat.plan.common.item.function.operator.cmpfunc;

import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;


/**
 * This Item represents a <code>X IS TRUE</code> boolean predicate.
 * 
 * 
 */
public class ItemFuncIsfalse extends ItemFuncTruth {

	public ItemFuncIsfalse(Item a) {
		super(a, false, true);
		args.add(a);
	}
	
	@Override
	public final String funcName() {
		return "isfalse";
	}
	
	@Override
	public SQLExpr toExpression() {
		SQLExpr left = args.get(0).toExpression();
		return new SQLBinaryOpExpr(left, SQLBinaryOperator.Is, new SQLBooleanExpr(false));
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		List<Item> newArgs = null;
		if (!forCalculate)
			newArgs = cloneStructList(args);
		else
			newArgs = calArgs;
		return new ItemFuncIsfalse(newArgs.get(0));
	}

}
