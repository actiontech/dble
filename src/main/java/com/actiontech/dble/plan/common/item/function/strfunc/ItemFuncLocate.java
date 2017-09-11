/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncLocate extends ItemIntFunc {

    public ItemFuncLocate(List<Item> args) {
        super(args);
    }

    @Override
    public String funcName() {
        return "locate";
    }

    @Override
    public BigInteger valInt() {
        String sub = args.get(0).valStr();
        String str = args.get(1).valStr();
        int pos = -1;
        if (args.size() == 3) {
            pos = args.get(2).valInt().intValue();
        }
        if (args.get(0).isNull() || args.get(1).isNull()) {
            this.nullValue = true;
            return BigInteger.ZERO;
        }
        if (pos <= 1) {
            return BigInteger.valueOf(str.indexOf(sub) + 1);
        } else {
            String posStr = str.substring(pos - 1);
            int find = posStr.indexOf(sub);
            if (find < 0)
                return BigInteger.ZERO;
            else
                return BigInteger.valueOf(pos + find);
        }
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncLocate(realArgs);
    }

}
