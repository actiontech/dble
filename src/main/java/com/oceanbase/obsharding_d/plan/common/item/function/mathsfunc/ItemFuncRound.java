/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.mathsfunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemInt;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;

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
