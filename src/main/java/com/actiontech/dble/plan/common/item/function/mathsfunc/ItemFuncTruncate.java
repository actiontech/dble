/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;

public class ItemFuncTruncate extends ItemFuncRoundOrTruncate {

    public ItemFuncTruncate(List<Item> args, int charsetIndex) {
        super(args, true, charsetIndex);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncTruncate(realArgs, charsetIndex);
    }
}
