/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.strfunc;


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
