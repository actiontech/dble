package io.mycat.plan.common.item.function.strfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncElt extends ItemStrFunc {

    public ItemFuncElt(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "elt";
    }

    @Override
    public String valStr() {
        Long l = args.get(0).valInt().longValue();
        if (l < 1 || l >= args.size()) {
            this.nullValue = true;
            return EMPTY;
        }
        Item arg = args.get(l.intValue());
        if (arg.isNull()) {
            this.nullValue = true;
            return EMPTY;
        } else {
            return arg.valStr();
        }
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncElt(realArgs);
    }
}
