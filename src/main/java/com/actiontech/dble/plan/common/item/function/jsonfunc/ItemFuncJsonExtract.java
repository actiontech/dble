package com.actiontech.dble.plan.common.item.function.jsonfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.primary.ItemJsonFunc;

import java.util.List;

public class ItemFuncJsonExtract extends ItemJsonFunc {

    public ItemFuncJsonExtract(List<Item> args) {
        super(args);
    }

    @Override
    public String funcName() {
        return "JSON_EXTRACT";
    }

    @Override
    public String valStr() {
        //todo:
        return args.get(0).valStr();
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncJsonExtract(realArgs);
    }
}
