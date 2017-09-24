/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemfuncLoadFile extends ItemStrFunc {

    public ItemfuncLoadFile(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "load_file";
    }

    @Override
    public String valStr() {
        throw new RuntimeException("LOAD_FILE function is not realized");
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemfuncLoadFile(realArgs);
    }
}
