/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.jsonfunc;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.strfunc.ItemStrFunc;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.util.List;

/**
 * @author dcy
 * Create Date: 2022-01-24
 */
public class ItemFuncJsonUnQuote extends ItemStrFunc {
    public static final char QUOTE = '"';

    public ItemFuncJsonUnQuote(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    public ItemFuncJsonUnQuote(Item a, int charsetIndex) {
        super(a, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "json_unquote";
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncJsonUnQuote(realArgs, charsetIndex);
    }


    @Override
    public String valStr() {
        final Item arg1 = args.get(0);
        if (args.size() != 1) {
            throw new IllegalStateException("illegal argument count for json_unquote");
        }
        String inputStr = arg1.valStr();
        if (inputStr == null || arg1.isNullValue()) {
            this.nullValue = true;
            LOGGER.debug("use inner json_unquote() , use arg null");
            return EMPTY;
        }
        LOGGER.debug("use inner json_unquote() , use arg {}", inputStr);

        //exclude if not string
        if (inputStr.length() < 2 || inputStr.charAt(0) != QUOTE || inputStr.charAt(inputStr.length() - 1) != QUOTE) {
            this.nullValue = false;
            return inputStr;
        }

        final JsonElement parse = JsonParser.parseString(inputStr);
        if (parse.isJsonPrimitive() && parse.getAsJsonPrimitive().isString()) {
            inputStr = parse.getAsString();
        }

        if (inputStr == null) {
            this.nullValue = true;
            return EMPTY;
        }
        this.nullValue = false;
        return inputStr;
    }

}
