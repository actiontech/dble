/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.io.ByteArrayOutputStream;
import java.util.List;

public class ItemFuncUnhex extends ItemStrFunc {

    public ItemFuncUnhex(Item a) {
        super(a);
    }

    @Override
    public final String funcName() {
        return "unhex";
    }

    @Override
    public void fixLengthAndDec() {
        decimals = 0;
        maxLength = (1 + args.get(0).getMaxLength()) / 2;
    }

    @Override
    public String valStr() {
        nullValue = true;
        String hexString = args.get(0).valStr();
        if (args.get(0).isNullValue())
            return null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(hexString.length() / 2);
        for (int index = 0; index < hexString.length(); index += 2) {
            int hexChar = 0;
            int bValue = 0;
            bValue = ((hexChar = hexcharToInt(hexString.charAt(index))) << 4);
            if (hexChar == -1)
                return null;
            bValue |= (hexChar = hexcharToInt(hexString.charAt(index + 1)));
            if (hexChar == -1)
                return null;
            baos.write(bValue);
        }
        nullValue = false;
        return baos.toString();
    }

    /*
     * decode form hexString to String ,include chinese
     */
    public static String decode(String hexString) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(hexString.length() / 2);
        for (int i = 0; i < hexString.length(); i += 2)
            baos.write((hexString.indexOf(hexString.charAt(i)) << 4 | hexString.indexOf(hexString.charAt(i + 1))));
        return new String(baos.toByteArray());
    }

    /**
     * convert a hex digit into number.
     */

    public static int hexcharToInt(char c) {
        if (c <= '9' && c >= '0')
            return c - '0';
        c |= 32;
        if (c <= 'f' && c >= 'a')
            return c - 'a' + 10;
        return -1;
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncUnhex(realArgs.get(0));
    }

}
