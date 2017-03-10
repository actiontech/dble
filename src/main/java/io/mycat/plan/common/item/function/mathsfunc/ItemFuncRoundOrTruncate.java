package io.mycat.plan.common.item.function.mathsfunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.primary.ItemFuncNum1;

/**
 * round和truncate的父类
 
 */
public abstract class ItemFuncRoundOrTruncate extends ItemFuncNum1 {
	boolean truncate = false;

	public ItemFuncRoundOrTruncate(List<Item> args, boolean truncate) {
		super(args);
		this.truncate = truncate;
	}

	@Override
	public final String funcName() {
		return truncate ? "truncate" : "round";
	}

	@Override
	public BigDecimal realOp() {
		BigDecimal val0 = args.get(0).valReal();
		if (!(nullValue = args.get(0).isNull() || args.get(1).isNull())) {
			int val1 = args.get(1).valInt().intValue();
			return getDecimalRound(val0, val1);
		}
		return BigDecimal.ZERO;
	}

	@Override
	public BigInteger intOp() {
		/**
		 * round(1234,3) = 1234 round(1234,-1) = 1230
		 */
		BigInteger val0 = args.get(0).valInt();
		int val1 = args.get(1).valInt().intValue();
		if ((nullValue = args.get(0).nullValue || args.get(1).nullValue))
			return BigInteger.ZERO;
		return getIntRound(val0, val1);
	}

	@Override
	public BigDecimal decimalOp() {
		hybrid_type = ItemResult.DECIMAL_RESULT;
		if (args.get(0).isNull() || args.get(1).isNull()) {
			this.nullValue = true;
			return null;
		}
		BigDecimal val0 = args.get(0).valDecimal();
		int val1 = args.get(1).valInt().intValue();
		return getDecimalRound(val0, val1);
	}

	@Override
	public void fixLengthAndDec() {
		int decimals_to_set;
		long val1 = args.get(1).valInt().longValue();
		if ((nullValue = args.get(1).isNull()))
			return;

		if (val1 < 0)
			decimals_to_set = 0;
		else
			decimals_to_set = (int) val1;

		if (args.get(0).decimals == NOT_FIXED_DEC) {
			decimals = Math.min(decimals_to_set, NOT_FIXED_DEC);
			maxLength = floatLength(decimals);
			hybrid_type = ItemResult.REAL_RESULT;
			return;
		}

		switch (args.get(0).resultType()) {
		case REAL_RESULT:
		case STRING_RESULT:
			hybrid_type = ItemResult.REAL_RESULT;
			decimals = Math.min(decimals_to_set, NOT_FIXED_DEC);
			maxLength = floatLength(decimals);
			break;
		case INT_RESULT:
			/* Here we can keep INT_RESULT */
			hybrid_type = ItemResult.INT_RESULT;
			decimals = 0;
			break;
		/* fall through */
		case DECIMAL_RESULT: {
			hybrid_type = ItemResult.DECIMAL_RESULT;
			decimals_to_set = Math.min(DECIMAL_MAX_SCALE, decimals_to_set);
			decimals = Math.min(decimals_to_set, DECIMAL_MAX_SCALE);
			break;
		}
		default:
			assert (false); /* This result type isn't handled */
		}
	}

	/**
	 * round(1234,3) = 1234 round(-1234,-1) = -1230
	 * 
	 * @param value
	 * @param round
	 * @return
	 */
	private BigInteger getIntRound(BigInteger value, int round) {
		if (round >= 0)
			return value;
		round = -round;
		String sval = value.toString();
		int maxLen = value.compareTo(BigInteger.ZERO) >= 0 ? sval.length() : sval.length() - 1;
		if (round >= maxLen)
			return BigInteger.ZERO;
		String appendZero = org.apache.commons.lang.StringUtils.repeat("0", round);
		String subVal0 = sval.substring(sval.length() - round);
		String res = subVal0 + appendZero;
		return new BigInteger(res);
	}

	private BigDecimal getDecimalRound(BigDecimal value, int round) {
		String sVal = value.toString();
		if (!sVal.contains(".") || round < 0) {
			BigInteger bi = value.toBigInteger();
			return new BigDecimal(getIntRound(bi, round));
		} else {
			return value.setScale(round, RoundingMode.FLOOR);
		}
	}
}
