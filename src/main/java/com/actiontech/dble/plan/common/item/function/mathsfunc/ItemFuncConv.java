/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemStrFunc;

import java.util.List;


public class ItemFuncConv extends ItemStrFunc {
    public ItemFuncConv(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "conv";
    }

    @Override
    public String valStr() {
        String res = args.get(0).valStr();
        int fromBase = args.get(1).valInt().intValue();
        int toBase = args.get(2).valInt().intValue();
        long dec = 0;

        if (args.get(0).isNullValue() || args.get(1).isNullValue() || args.get(2).isNullValue() || Math.abs(toBase) > 36 ||
                Math.abs(toBase) < 2 || Math.abs(fromBase) > 36 || Math.abs(fromBase) < 2 || res.length() == 0) {
            nullValue = true;
            return null;
        }
        nullValue = false;
        if (args.get(0).fieldType() == FieldTypes.MYSQL_TYPE_BIT) {
            /*
             * Special case: The string representation of BIT doesn't resemble
             * the decimal representation, so we shouldn't change it to string
             * and then to decimal.
             */
            dec = args.get(0).valInt().longValue();
        } else {
            if (fromBase < 0)
                fromBase = -fromBase;
            try {
                dec = Long.parseLong(res, fromBase);
            } catch (NumberFormatException ne) {
                LOGGER.info("long parse from radix error, string:" + res + ", radix:" + fromBase);
            }
        }

        String str = null;
        try {
            str = Long.toString(dec, toBase);
        } catch (Exception e) {
            LOGGER.warn("long to string failed ,value:" + dec + ", to_base:" + toBase);
            nullValue = true;
        }
        return str;
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 64;
        maybeNull = true;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncConv(realArgs);
    }

}
