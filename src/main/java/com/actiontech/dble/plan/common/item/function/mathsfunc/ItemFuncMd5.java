/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.plan.common.item.function.mathsfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemStrFunc;

import java.util.List;


public class ItemFuncMd5 extends ItemStrFunc {

    /**
     * @param a
     */
    public ItemFuncMd5(Item a, int charsetIndex) {
        super(a, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "md5";
    }

    @Override
    public String valStr() {
        String value = args.get(0).valStr();
        if (value != null) {
            nullValue = false;

        }
        // TODO
        return null;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncMd5(realArgs.get(0), charsetIndex);
    }
}
