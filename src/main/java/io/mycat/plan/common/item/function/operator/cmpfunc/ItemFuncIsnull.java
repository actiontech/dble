package io.mycat.plan.common.item.function.operator.cmpfunc;

import java.math.BigInteger;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemBoolFunc;


/*
 * 当MySQL的sql_auto_is_null变量设为true,并且col_name为自增列时，
 * select * from table_name where col_name is null返回last_insert_id
 */
public class ItemFuncIsnull extends ItemBoolFunc {

	public ItemFuncIsnull(Item a) {
		super(a);
	}

	@Override
	public final String funcName() {
		return "isnull";
	}

	@Override
	public Functype functype() {
		return Functype.ISNULL_FUNC;
	}

	@Override
	public BigInteger valInt() {
		if (args.get(0).isNull()) {
			return BigInteger.ONE;
		} else {
			return BigInteger.ZERO;
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
		return new SQLBinaryOpExpr(left, SQLBinaryOperator.Is, new SQLIdentifierExpr("UNKNOWN"));
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		List<Item> newArgs = null;
		if (!forCalculate)
			newArgs = cloneStructList(args);
		else
			newArgs = calArgs;
		return new ItemFuncIsnull(newArgs.get(0));
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncIsnull(realArgs.get(0));
	}

}
