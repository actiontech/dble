package io.mycat.plan.common.item.function.strfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;

import java.util.List;


public class ItemFuncConcatWs extends ItemStrFunc {

    public ItemFuncConcatWs(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "concat_ws";
    }

    @Override
    public String valStr() {
        StringBuilder sb = new StringBuilder();
        String sep = args.get(0).valStr();
        for (int i = 1; i < args.size(); i++) {
            Item arg = args.get(i);
            if (!arg.isNull()) {
                String s = arg.valStr();
                sb.append(s);
                if (i < args.size() - 1) {
                    sb.append(sep);
                }
            }
        }
        return sb.toString();
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncConcatWs(realArgs);
    }

}
