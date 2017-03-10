package io.mycat.plan.common.item.function.strfunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;

public class ItemFuncField extends ItemIntFunc {

	ItemResult cmp_type;

	public ItemFuncField(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "field";
	}

	@Override
	public BigInteger valInt() {
		if (cmp_type == ItemResult.STRING_RESULT) {
			String field;
			if ((field = args.get(0).valStr()) == null)
				return BigInteger.ZERO;
			for (int i = 1; i < args.size(); i++) {
				String tmp_value = args.get(i).valStr();
				if (tmp_value != null && field.compareTo(tmp_value) == 0)
					return BigInteger.valueOf(i);
			}
		} else if (cmp_type == ItemResult.INT_RESULT) {
			long val = args.get(0).valInt().longValue();
			if (args.get(0).nullValue)
				return BigInteger.ZERO;
			for (int i = 1; i < getArgCount(); i++) {
				if (val == args.get(i).valInt().longValue() && !args.get(i).nullValue)
					return BigInteger.valueOf(i);
			}
		} else if (cmp_type == ItemResult.DECIMAL_RESULT) {
			BigDecimal dec = args.get(0).valDecimal();
			if (args.get(0).nullValue)
				return BigInteger.ZERO;
			for (int i = 1; i < getArgCount(); i++) {
				BigDecimal dec_arg = args.get(i).valDecimal();
				if (!args.get(i).nullValue && dec_arg.compareTo(dec) == 0)
					return BigInteger.valueOf(i);
			}
		} else {
			double val = args.get(0).valReal().doubleValue();
			if (args.get(0).nullValue)
				return BigInteger.ZERO;
			for (int i = 1; i < getArgCount(); i++) {
				if (val == args.get(i).valReal().doubleValue() && !args.get(i).nullValue)
					return BigInteger.valueOf(i);
			}
		}
		return BigInteger.ZERO;
	}

	@Override
	public void fixLengthAndDec() {
		maybeNull = false;
		maxLength = 3;
		cmp_type = args.get(0).resultType();
		for (int i = 1; i < args.size(); i++)
			cmp_type = MySQLcom.item_cmp_type(cmp_type, args.get(i).resultType());
	}
	
	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncField(realArgs);
	}
}
