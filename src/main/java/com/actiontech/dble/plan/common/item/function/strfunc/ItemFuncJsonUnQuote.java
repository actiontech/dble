package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
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
        if (arg1.isNull()) {
            this.nullValue = true;
            LOGGER.debug("use inner json_unquote() , use arg null");
            return EMPTY;
        }
        if (args.size() != 1) {
            throw new IllegalStateException("illegal argument count for json_unquote");
        }
        String inputStr = arg1.valStr();
        LOGGER.debug("use inner json_unquote() , use arg {}", inputStr);

        if (inputStr == null) {
            this.nullValue = true;
            return EMPTY;
        }
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
