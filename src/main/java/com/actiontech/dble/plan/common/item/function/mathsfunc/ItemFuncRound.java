/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncRound extends ItemFuncRoundOrTruncate {

    public ItemFuncRound(List<Item> args, int charsetIndex) {
        super(args, false, charsetIndex);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        if (realArgs != null && realArgs.size() == 1) {
            realArgs.add(new ItemInt(0));
        }
        return new ItemFuncRound(realArgs, charsetIndex);
    }

}
