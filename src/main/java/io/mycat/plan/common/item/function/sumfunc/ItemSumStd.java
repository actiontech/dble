package io.mycat.plan.common.item.function.sumfunc;

import java.math.BigDecimal;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;


/**
 * standard_deviation(a) = sqrt(variance(a))
 * 
 * 
 */
public class ItemSumStd extends ItemSumVariance {

	public ItemSumStd(List<Item> args, int sample, boolean isPushDown, List<Field> fields) {
		super(args, sample, isPushDown, fields);
	}

	@Override
	public Sumfunctype sumType() {
		return Sumfunctype.STD_FUNC;
	}

	@Override
	public BigDecimal valReal() {
		BigDecimal val = super.valReal();
		double db = Math.sqrt(val.doubleValue());
		return BigDecimal.valueOf(db);
	}

	@Override
	public String funcName() {
		return sample == 1 ? "STDDEV_SAMP" : "STD";
	}

	@Override
	public ItemResult resultType() {
		return ItemResult.REAL_RESULT;
	}

	@Override
	public FieldTypes fieldType() {
		return FieldTypes.MYSQL_TYPE_DOUBLE;
	}

	@Override
	public SQLExpr toExpression() {
		SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName());
		for (Item arg : args) {
			method.addParameter(arg.toExpression());
		}
		return method;
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		if (!forCalculate) {
			List<Item> newArgs = cloneStructList(args);
			return new ItemSumStd(newArgs, sample, false, null);
		} else {
			return new ItemSumStd(calArgs, sample, isPushDown, fields);
		}
	}

}
