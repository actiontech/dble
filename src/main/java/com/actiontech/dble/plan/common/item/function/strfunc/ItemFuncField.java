/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemFuncField extends ItemIntFunc {

    ItemResult cmpType;

    public ItemFuncField(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "field";
    }

    @Override
    public BigInteger valInt() {
        if (cmpType == ItemResult.STRING_RESULT) {
            String field;
            if ((field = args.get(0).valStr()) == null)
                return BigInteger.ZERO;
            for (int i = 1; i < args.size(); i++) {
                String tmpValue = args.get(i).valStr();
                if (tmpValue != null && field.compareTo(tmpValue) == 0)
                    return BigInteger.valueOf(i);
            }
        } else if (cmpType == ItemResult.INT_RESULT) {
            long val = args.get(0).valInt().longValue();
            if (args.get(0).isNullValue())
                return BigInteger.ZERO;
            for (int i = 1; i < getArgCount(); i++) {
                if (val == args.get(i).valInt().longValue() && !args.get(i).isNullValue())
                    return BigInteger.valueOf(i);
            }
        } else if (cmpType == ItemResult.DECIMAL_RESULT) {
            BigDecimal dec = args.get(0).valDecimal();
            if (args.get(0).isNullValue())
                return BigInteger.ZERO;
            for (int i = 1; i < getArgCount(); i++) {
                BigDecimal decArg = args.get(i).valDecimal();
                if (!args.get(i).isNullValue() && decArg.compareTo(dec) == 0)
                    return BigInteger.valueOf(i);
            }
        } else {
            double val = args.get(0).valReal().doubleValue();
            if (args.get(0).isNullValue())
                return BigInteger.ZERO;
            for (int i = 1; i < getArgCount(); i++) {
                if (val == args.get(i).valReal().doubleValue() && !args.get(i).isNullValue())
                    return BigInteger.valueOf(i);
            }
        }
        return BigInteger.ZERO;
    }

    @Override
    public void fixLengthAndDec() {
        maybeNull = false;
        maxLength = 3;
        cmpType = args.get(0).resultType();
        for (int i = 1; i < args.size(); i++)
            cmpType = MySQLcom.itemCmpType(cmpType, args.get(i).resultType());
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncField(realArgs, charsetIndex);
    }
}
