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
            return null;
        }
        String inputStr = arg1.valStr();

        if (inputStr == null) {
            this.nullValue = true;
            return null;
        }
        final JsonElement parse = new JsonParser().parse(inputStr);
        if (parse.isJsonPrimitive() && parse.getAsJsonPrimitive().isString()) {
            inputStr = parse.getAsString();
        }

        if (inputStr == null) {
            this.nullValue = true;
            return null;
        }
        this.nullValue = false;
        return inputStr;
    }

}
