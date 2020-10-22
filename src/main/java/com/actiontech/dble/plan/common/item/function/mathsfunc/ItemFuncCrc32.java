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
import com.actiontech.dble.plan.common.item.function.primary.ItemIntFunc;

import java.math.BigInteger;
import java.util.List;
import java.util.zip.CRC32;

public class ItemFuncCrc32 extends ItemIntFunc {

    /**
     * @param a
     */
    public ItemFuncCrc32(Item a, int charsetIndex) {
        super(a, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "crc32";
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 10;
    }

    @Override
    public BigInteger valInt() {
        String res = args.get(0).valStr();
        if (res == null) {
            nullValue = true;
            return BigInteger.ZERO;
        }
        nullValue = false;
        CRC32 crc = new CRC32();
        crc.update(res.getBytes());
        return BigInteger.valueOf(crc.getValue());
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncCrc32(realArgs.get(0), charsetIndex);
    }
}
