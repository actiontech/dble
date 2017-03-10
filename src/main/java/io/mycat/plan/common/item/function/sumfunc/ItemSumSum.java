package io.mycat.plan.common.item.function.sumfunc;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;

import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;


public class ItemSumSum extends ItemSumNum {

	public ItemSumSum(List<Item> args, boolean distinct, boolean isPushDown, List<Field> fields) {
		super(args, isPushDown, fields);
		setDistinct(distinct);
	}

	protected ItemResult hybrid_type;
	protected BigDecimal sum;

	@Override
	public void fixLengthAndDec() {
		maybeNull = nullValue = true;
		decimals = args.get(0).decimals;
		switch (args.get(0).numericContextResultType()) {
		case REAL_RESULT:
			hybrid_type = ItemResult.REAL_RESULT;
			sum = BigDecimal.ZERO;
			break;
		case INT_RESULT:
		case DECIMAL_RESULT: {
			/* SUM result can't be longer than length(arg) + length(MAX_ROWS) */
			int precision = args.get(0).decimalPrecision() + MySQLcom.DECIMAL_LONGLONG_DIGITS;
			maxLength = precision + 2;// 一个小数点一个负号
			hybrid_type = ItemResult.DECIMAL_RESULT;
			sum = BigDecimal.ZERO;
			break;
		}
		case STRING_RESULT:
		case ROW_RESULT:
		default:
			assert (false);
		}
	}

	public Sumfunctype sumType() {
		return has_with_distinct() ? Sumfunctype.SUM_DISTINCT_FUNC : Sumfunctype.SUM_FUNC;
	}

	@Override
	public void clear() {
		nullValue = true;
		sum = BigDecimal.ZERO;
	}

	protected static class AggData implements Serializable {

		private static final long serialVersionUID = 6951860386146676307L;

		public BigDecimal bd;
		public boolean isNull;

		public AggData(BigDecimal bd, boolean isNull) {
			this.bd = bd;
			this.isNull = isNull;
		}

	}

	@Override
	public Object getTransAggObj() {
		AggData data = new AggData(sum, nullValue);
		return data;
	}

	@Override
	public int getTransSize() {
		return 10;
	}

	@Override
	public boolean add(RowDataPacket row, Object transObj) {
		if (transObj != null) {
			AggData data = (AggData) transObj;
			if (hybrid_type == ItemResult.DECIMAL_RESULT) {
				final BigDecimal val = data.bd;
				if (!data.isNull) {
					sum = sum.add(val);
					nullValue = false;
				}
			} else {
				sum = sum.add(data.bd);
				if (!data.isNull)
					nullValue = false;
			}
		} else {
			if (hybrid_type == ItemResult.DECIMAL_RESULT) {
				final BigDecimal val = aggr.arg_val_decimal();
				if (!aggr.arg_is_null()) {
					sum = sum.add(val);
					nullValue = false;
				}
			} else {
				sum = sum.add(aggr.arg_val_real());
				if (!aggr.arg_is_null())
					nullValue = false;
			}
		}
		return false;
	}

	/**
	 * sum(id)的pushdown为sum(id)
	 */
	@Override
	public boolean pushDownAdd(RowDataPacket row) {
		if (hybrid_type == ItemResult.DECIMAL_RESULT) {
			final BigDecimal val = aggr.arg_val_decimal();
			if (!aggr.arg_is_null()) {
				sum = sum.add(val);
				nullValue = false;
			}
		} else {
			sum = sum.add(aggr.arg_val_real());
			if (!aggr.arg_is_null())
				nullValue = false;
		}
		return false;
	}

	@Override
	public BigDecimal valReal() {
		if (aggr != null) {
			aggr.endup();
		}
		return sum;
	}

	@Override
	public ItemResult resultType() {
		return hybrid_type;
	}

	@Override
	public String funcName() {
		return "SUM";
	}

	@Override
	public SQLExpr toExpression() {
		Item arg0 = getArg(0);
		SQLAggregateExpr aggregate = new SQLAggregateExpr(funcName());
		aggregate.addArgument(arg0.toExpression());
		if(has_with_distinct()){
			aggregate.setOption(SQLAggregateOption.DISTINCT);
		}
		return aggregate;
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		if (!forCalculate) {
			List<Item> newArgs = cloneStructList(args);
			return new ItemSumSum(newArgs, has_with_distinct(), false, null);
		} else {
			return new ItemSumSum(calArgs, has_with_distinct(), isPushDown, fields);
		}
	}
}
