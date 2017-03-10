package io.mycat.plan.common.item.function.operator.cmpfunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;

import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.operator.cmpfunc.util.ArgComparator;
import io.mycat.plan.common.item.function.operator.cmpfunc.util.CmpUtil;
import io.mycat.plan.common.ptr.ItemResultPtr;


public class ItemFuncBetweenAnd extends ItemFuncOptNeg {
	private ItemResult cmp_type;
	String value0, value1, value2;
	/* TRUE <=> arguments will be compared as dates. */
	boolean compare_as_dates_with_strings;
	boolean compare_as_temporal_dates;
	boolean compare_as_temporal_times;

	/* Comparators used for DATE/DATETIME comparison. */
	ArgComparator ge_cmp, le_cmp;

	/**
	 * select 'a' in ('a','b','c') args(0)为'a',[1]为'a',[2]为'b'。。。
	 * 
	 * @param args
	 */
	public ItemFuncBetweenAnd(Item a, Item b, Item c, boolean isNegation) {
		super(new ArrayList<Item>(), isNegation);
		args.add(a);
		args.add(b);
		args.add(c);
	}

	@Override
	public final String funcName() {
		return "between";
	}

	@Override
	public Functype functype() {
		return Functype.BETWEEN;
	}

	@Override
	public BigInteger valInt() {
		if (compare_as_dates_with_strings) {
			int ge_res, le_res;

			ge_res = ge_cmp.compare();
			if ((nullValue = args.get(0).isNull()))
				return BigInteger.ZERO;
			le_res = le_cmp.compare();

			if (!args.get(1).isNull() && !args.get(2).isNull())
				return ((ge_res >= 0 && le_res <= 0)) != negated ? BigInteger.ONE : BigInteger.ZERO;
			else if (args.get(1).isNull()) {
				nullValue = le_res > 0; // not null if false range.
			} else {
				nullValue = ge_res < 0;
			}
		} else if (cmp_type == ItemResult.STRING_RESULT) {
			String value, a, b;
			value = args.get(0).valStr();
			if (nullValue = args.get(0).isNull())
				return BigInteger.ZERO;
			a = args.get(1).valStr();
			b = args.get(2).valStr();
			if (!args.get(1).isNull() && !args.get(2).isNull())
				return (value.compareTo(a) >= 0 && value.compareTo(b) <= 0) != negated ? BigInteger.ONE
						: BigInteger.ZERO;
			if (args.get(1).isNull() && args.get(2).isNull())
				nullValue = true;
			else if (args.get(1).isNull()) {
				// Set to not null if false range.
				nullValue = value.compareTo(b) <= 0;
			} else {
				// Set to not null if false range.
				nullValue = value.compareTo(a) >= 0;
			}
		} else if (cmp_type == ItemResult.INT_RESULT) {
			long a, b, value;
			value = compare_as_temporal_times ? args.get(0).valTimeTemporal()
					: compare_as_temporal_dates ? args.get(0).valDateTemporal() : args.get(0).valInt().longValue();
			if (nullValue = args.get(0).isNull())
				return BigInteger.ZERO; /* purecov: inspected */
			if (compare_as_temporal_times) {
				a = args.get(1).valTimeTemporal();
				b = args.get(2).valTimeTemporal();
			} else if (compare_as_temporal_dates) {
				a = args.get(1).valDateTemporal();
				b = args.get(2).valDateTemporal();
			} else {
				a = args.get(1).valInt().longValue();
				b = args.get(2).valInt().longValue();
			}
			if (!args.get(1).isNull() && !args.get(2).isNull())
				return (value >= a && value <= b) != negated ? BigInteger.ONE : BigInteger.ZERO;
			if (args.get(1).isNull() && args.get(2).isNull())
				nullValue = true;
			else if (args.get(1).isNull()) {
				nullValue = value <= b; // not null if false range.
			} else {
				nullValue = value >= a;
			}
		} else if (cmp_type == ItemResult.DECIMAL_RESULT) {
			BigDecimal dec = args.get(0).valDecimal();
			BigDecimal a_dec, b_dec;
			if (nullValue = args.get(0).isNull())
				return BigInteger.ZERO; /* purecov: inspected */
			a_dec = args.get(1).valDecimal();
			b_dec = args.get(2).valDecimal();
			if (!args.get(1).isNull() && !args.get(2).isNull())
				return (dec.compareTo(a_dec) >= 0 && dec.compareTo(b_dec) <= 0) != negated ? BigInteger.ONE
						: BigInteger.ZERO;
			if (args.get(1).isNull() && args.get(2).isNull())
				nullValue = true;
			else if (args.get(1).isNull())
				nullValue = dec.compareTo(b_dec) <= 0;
			else
				nullValue = dec.compareTo(a_dec) >= 0;
		} else {
			double value = args.get(0).valReal().doubleValue(), a, b;
			if (nullValue = args.get(0).isNull())
				return BigInteger.ZERO; /* purecov: inspected */
			a = args.get(1).valReal().doubleValue();
			b = args.get(2).valReal().doubleValue();
			if (!args.get(1).isNull() && !args.get(2).isNull())
				return (value >= a && value <= b) != negated ? BigInteger.ONE : BigInteger.ZERO;
			if (args.get(1).isNull() && args.get(2).isNull())
				nullValue = true;
			else if (args.get(1).isNull()) {
				nullValue = value <= b; // not null if false range.
			} else {
				nullValue = value >= a;
			}
		}
		return !nullValue ? BigInteger.ONE : BigInteger.ZERO;

	}

	@Override
	public boolean fixFields() {
		if (super.fixFields())
			return true;

		return false;
	}

	@Override
	public void fixLengthAndDec() {
		maxLength = 1;
		int i;
		int datetime_items_found = 0;
		int time_items_found = 0;
		compare_as_dates_with_strings = false;
		compare_as_temporal_times = compare_as_temporal_dates = false;
		/*
		 * As some compare functions are generated after sql_yacc, we have to
		 * check for out of memory conditions here
		 */
		if (args.get(0) == null || args.get(1) == null || args.get(2) == null)
			return;
		if (CmpUtil.agg_cmp_type(new ItemResultPtr(cmp_type), args, 3) != 0)
			return;
		/*
		 * Detect the comparison of DATE/DATETIME items. At least one of items
		 * should be a DATE/DATETIME item and other items should return the
		 * STRING result.
		 */
		if (cmp_type == ItemResult.STRING_RESULT) {
			for (i = 0; i < 3; i++) {
				if (args.get(i).isTemporalWithDate())
					datetime_items_found++;
				else if (args.get(i).fieldType() == FieldTypes.MYSQL_TYPE_TIME)
					time_items_found++;
			}
		}

		if (datetime_items_found + time_items_found == 3) {
			if (time_items_found == 3) {
				// All items are TIME
				cmp_type = ItemResult.INT_RESULT;
				compare_as_temporal_times = true;
			} else {
				/*
				 * There is at least one DATE or DATETIME item, all other items
				 * are DATE, DATETIME or TIME.
				 */
				cmp_type = ItemResult.INT_RESULT;
				compare_as_temporal_dates = true;
			}
		} else if (datetime_items_found > 0) {
			/*
			 * There is at least one DATE or DATETIME item. All other items are
			 * DATE, DATETIME or strings.
			 */
			compare_as_dates_with_strings = true;
			ge_cmp.setDatetimeCmpFunc(this, args.get(0), args.get(1));
			le_cmp.setDatetimeCmpFunc(this, args.get(0), args.get(2));
		} else {
			// TODO
		}
	}

	@Override
	public int decimalPrecision() {
		return 1;
	}

	@Override
	public SQLExpr toExpression() {
		SQLExpr first = args.get(0).toExpression();
		SQLExpr second = args.get(1).toExpression();
		SQLExpr third = args.get(2).toExpression();
		return new SQLBetweenExpr(first,this.negated, second, third);
	}

	@Override
	protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
		List<Item> newArgs = null;
		if (!forCalculate)
			newArgs = cloneStructList(args);
		else
			newArgs = calArgs;
		return new ItemFuncBetweenAnd(newArgs.get(0), newArgs.get(1), newArgs.get(2), this.negated);
	}
}
