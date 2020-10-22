/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemFuncNumhybrid;
import com.actiontech.dble.plan.common.time.MySQLTime;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


/*
 * ItemFuncCoalesce
 */
public class ItemFuncCoalesce extends ItemFuncNumhybrid {

    protected FieldTypes cachedFieldType;

    public ItemFuncCoalesce(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public String funcName() {
        return "coalesce";
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncCoalesce(realArgs, charsetIndex);
    }

    @Override
    public void fixLengthAndDec() {
        cachedFieldType = MySQLcom.aggFieldType(args, 0, args.size());
        hybridType = MySQLcom.aggResultType(args, 0, args.size());
        if (hybridType == ItemResult.STRING_RESULT) {
            //do nothing
        } else if (hybridType == ItemResult.DECIMAL_RESULT) {
            countDecimalLength();
        } else if (hybridType == ItemResult.REAL_RESULT) {
            countRealLength();
        } else if (hybridType == ItemResult.INT_RESULT) {
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
            if (!args.get(i).isNullValue())
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
            if (!args.get(i).isNullValue())
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
            if (!args.get(i).isNullValue())
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
        return hybridType;
    }

    @Override
    public FieldTypes fieldType() {
        return cachedFieldType;
    }

}
