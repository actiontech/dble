/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncMakeSet extends ItemStrFunc {

    public ItemFuncMakeSet(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "make_set";
    }

    @Override
    public String valStr() {
        int bits = args.get(0).valInt().intValue();
        int nums = 1;
        StringBuilder sb = new StringBuilder();
        while (bits > 0 && nums < args.size()) {
            if (bits % 2 != 0) {
                String var = args.get(nums).valStr();
                if (var != null) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(var);
                }
            }
            bits = bits / 2;
            nums++;
        }
        return sb.toString();
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncMakeSet(realArgs);
    }
}
