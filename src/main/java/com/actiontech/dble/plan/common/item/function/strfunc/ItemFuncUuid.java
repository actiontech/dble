/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;


import java.util.UUID;

public class ItemFuncUuid extends ItemStrFunc {

    public ItemFuncUuid(int charsetIndex) {
        super(charsetIndex);
    }

    @Override
    public String funcName() {
        return "uuid";
    }

    @Override
    public String valStr() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void fixLengthAndDec() {
        fixCharLength(36);
    }
}
