/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.item.function.primary.ItemFuncNum1;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;

public abstract class ItemFuncRoundOrTruncate extends ItemFuncNum1 {
    boolean truncate = false;

    public ItemFuncRoundOrTruncate(List<Item> args, boolean truncate, int charsetIndex) {
        super(args, charsetIndex);
        if (this.args != null && this.args.size() == 1) {
            this.args.add(new ItemInt(0));
        }
        this.truncate = truncate;
    }

    @Override
    public final String funcName() {
        return truncate ? "truncate" : "round";
    }

    @Override
    public BigDecimal realOp() {
        BigDecimal val0 = args.get(0).valReal();
        if (!(nullValue = (args.get(0).isNull() || args.get(1).isNull()))) {
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
        if (nullValue = (args.get(0).isNullValue() || args.get(1).isNullValue()))
            return BigInteger.ZERO;
        return getIntRound(val0, val1);
    }

    @Override
    public BigDecimal decimalOp() {
        hybridType = ItemResult.DECIMAL_RESULT;
        if (args.get(0).isNull() || args.get(1).isNull()) {
            this.nullValue = true;
            return BigDecimal.ZERO;
        }
        BigDecimal val0 = args.get(0).valDecimal();
        int val1 = args.get(1).valInt().intValue();
        return getDecimalRound(val0, val1);
    }

    @Override
    public void fixLengthAndDec() {
        int decimalsToSet;
        long val1 = args.get(1).valInt().longValue();
        if ((nullValue = args.get(1).isNull()))
            return;

        if (val1 < 0)
            decimalsToSet = 0;
        else
            decimalsToSet = (int) val1;

        if (args.get(0).getDecimals() == NOT_FIXED_DEC) {
            decimals = Math.min(decimalsToSet, NOT_FIXED_DEC);
            maxLength = floatLength(decimals);
            hybridType = ItemResult.REAL_RESULT;
            return;
        }

        ItemResult i = args.get(0).resultType();
        if (i == ItemResult.REAL_RESULT || i == ItemResult.STRING_RESULT) {
            hybridType = ItemResult.REAL_RESULT;
            decimals = Math.min(decimalsToSet, NOT_FIXED_DEC);
            maxLength = floatLength(decimals);

        } else if (i == ItemResult.INT_RESULT) { /* Here we can keep INT_RESULT */
            hybridType = ItemResult.INT_RESULT;
            decimals = 0;

        /* fall through */
        } else if (i == ItemResult.DECIMAL_RESULT) {
            hybridType = ItemResult.DECIMAL_RESULT;
            decimalsToSet = Math.min(DECIMAL_MAX_SCALE, decimalsToSet);
            decimals = Math.min(decimalsToSet, DECIMAL_MAX_SCALE);
        } else {
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
