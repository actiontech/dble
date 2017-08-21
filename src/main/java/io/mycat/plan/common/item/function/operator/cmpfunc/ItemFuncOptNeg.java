package io.mycat.plan.common.item.function.operator.cmpfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.primary.ItemIntFunc;

import java.util.List;


/*
The class Item_func_opt_neg is defined to factor out the functionality
common for the classes Item_func_between and Item_func_in. The objects
of these classes can express predicates or there negations.
The alternative approach would be to create pairs Item_func_between,
Item_func_notbetween and Item_func_in, Item_func_notin.

*/
public abstract class ItemFuncOptNeg extends ItemIntFunc {

    public boolean negated = false; /* <=> the item represents NOT <func> */
//    public boolean pred_level = false; /*
//                                         * <=> [NOT] <func> is used on a
//                                         * predicate level
//                                         */

    public ItemFuncOptNeg(List<Item> args, boolean isNegation) {
        super(args);
        if (isNegation)
            negate();
    }

    public void negate() {
        negated = !negated;
    }

}
