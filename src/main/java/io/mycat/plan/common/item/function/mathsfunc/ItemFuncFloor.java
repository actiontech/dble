package io.mycat.plan.common.item.function.mathsfunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;


public class ItemFuncFloor extends ItemFuncIntVal {

	public ItemFuncFloor(List<Item> args) {
		super(args);
	}

	@Override
	public final String funcName() {
		return "floor";
	}

	@Override
	public BigInteger intOp() {
		BigInteger result;
		switch (args.get(0).resultType()) {
		case INT_RESULT:
			result = args.get(0).valInt();
			nullValue = args.get(0).nullValue;
			break;
		case DECIMAL_RESULT: {
			BigDecimal dec = decimalOp();
			if (dec == null)
				result = BigInteger.ZERO;
			else
				result = dec.toBigInteger();
			break;
		}
		default:
			result = realOp().toBigInteger();
		}
		;
		return result;
	}

	@Override
	public BigDecimal realOp() {
		double value = args.get(0).valReal().doubleValue();
		nullValue = args.get(0).nullValue;
		return new BigDecimal(Math.floor(value));
	}

	@Override
	public BigDecimal decimalOp() {
		BigDecimal bd = args.get(0).valDecimal();
		if (nullValue = args.get(0).nullValue)
			return null;
		return bd.setScale(0, RoundingMode.FLOOR);
	}

	@Override
	public ItemFunc nativeConstruct(List<Item> realArgs) {
		return new ItemFuncFloor(realArgs);
	}
}
