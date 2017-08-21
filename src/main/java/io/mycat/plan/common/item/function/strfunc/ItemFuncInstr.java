/**
 *
 */
package io.mycat.plan.common.item.function.strfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;

import java.util.List;

public class ItemFuncInstr extends ItemFuncLocate {

    /**
     * @param args
     */
    public ItemFuncInstr(List<Item> args) {
        super(args);
    }

    @Override
    public final String funcName() {
        return "instr";
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncInstr(realArgs);
    }
}
