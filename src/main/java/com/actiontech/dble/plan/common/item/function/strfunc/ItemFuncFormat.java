/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

public class ItemFuncFormat extends ItemStrFunc {

    public ItemFuncFormat(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "FORMAT";
    }

    @Override
    public String valStr() {
        int pl = args.get(1).valInt().intValue();
        if (pl < 0)
            pl = 0;
        String local = "en_US";
        if (args.size() == 3)
            local = args.get(2).valStr();
        Locale loc = new Locale(local);
        NumberFormat f = DecimalFormat.getInstance(loc);
        if (args.get(0).isNull() || args.get(1).isNull()) {
            this.nullValue = true;
            return EMPTY;
        }
        BigDecimal bd = args.get(0).valDecimal();
        BigDecimal bdnew = bd.setScale(pl, RoundingMode.HALF_UP);
        return f.format(bdnew);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncFormat(realArgs, charsetIndex);
    }
}
