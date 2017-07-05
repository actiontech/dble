package io.mycat.plan.common.item.function.operator.cmpfunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.FieldTypes;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemFuncNumhybrid;
import io.mycat.plan.common.time.MySQLTime;


/*
 * 返回第一个不是null的值
 */
public class ItemFuncCoalesce extends ItemFuncNumhybrid {

	protected FieldTypes cached_field_type;

	public ItemFuncCoalesce(List<Item> args) {
		super(args);
	}

	@Override
	public String funcName() {
		return "coalesce";
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncCoalesce(realArgs);
	}

	@Override
	public void fixLengthAndDec() {
		cached_field_type = MySQLcom.agg_field_type(args, 0, args.size());
		hybrid_type = MySQLcom.agg_result_type(args, 0, args.size());
		if (hybrid_type == ItemResult.STRING_RESULT) {
		} else if (hybrid_type == ItemResult.DECIMAL_RESULT) {
			countDecimalLength();

		} else if (hybrid_type == ItemResult.REAL_RESULT) {
			countRealLength();

		} else if (hybrid_type == ItemResult.INT_RESULT) {
			decimals = 0;

		} else {
			assert (false);
		}
	}

	@Override
	public void findNumType() {
	}

	@Override
	public BigInteger intOp() {
		nullValue = false;
		for (int i = 0; i < getArgCount(); i++) {
			BigInteger res = args.get(i).valInt();
			if (!args.get(i).nullValue)
				return res;
		}
		nullValue = true;
		return BigInteger.ZERO;
	}

	@Override
	public BigDecimal realOp() {
		nullValue = false;
		for (int i = 0; i < getArgCount(); i++) {
			BigDecimal res = args.get(i).valReal();
			if (!args.get(i).nullValue)
				return res;
		}
		nullValue = true;
		return BigDecimal.ZERO;
	}

	@Override
	public BigDecimal decimalOp() {
		nullValue = false;
		for (int i = 0; i < getArgCount(); i++) {
			BigDecimal res = args.get(i).valDecimal();
			if (!args.get(i).nullValue)
				return res;
		}
		nullValue = true;
		return null;
	}

	@Override
	public String strOp() {
		nullValue = false;
		for (int i = 0; i < getArgCount(); i++) {
			String res = args.get(i).valStr();
			if (res != null)
				return res;
		}
		nullValue = true;
		return null;
	}

	@Override
	public boolean dateOp(MySQLTime ltime, long fuzzydate) {
		for (int i = 0; i < getArgCount(); i++) {
			if (!args.get(i).getDate(ltime, fuzzydate))
				return (nullValue = false);
		}
		return (nullValue = true);
	}

	@Override
	public boolean timeOp(MySQLTime ltime) {
		for (int i = 0; i < getArgCount(); i++) {
			if (!args.get(i).getTime(ltime))
				return (nullValue = false);
		}
		return (nullValue = true);
	}

	@Override
	public ItemResult resultType() {
		return hybrid_type;
	}

	@Override
	public FieldTypes fieldType() {
		return cached_field_type;
	}

}
