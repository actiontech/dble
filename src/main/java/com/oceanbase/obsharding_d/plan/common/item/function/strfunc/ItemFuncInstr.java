/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.oceanbase.obsharding_d.plan.common.item.function.strfunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;

import java.util.List;

public class ItemFuncInstr extends ItemFuncLocate {

    /**
     * @param args
     */
    public ItemFuncInstr(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "instr";
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncInstr(realArgs, charsetIndex);
    }
}
