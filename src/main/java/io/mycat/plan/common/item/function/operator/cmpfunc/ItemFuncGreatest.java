package io.mycat.plan.common.item.function.operator.cmpfunc;

import java.util.List;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;


/*
 * select least(c1,c2,'5') from t,result is 5
 * select least(c1,c2,'a') from t,result is 'a'
 * select least(c1,c2,'a')+1 from t,result is 1
 * TODO: can we jude the return typ from args？
 * do we need to save thE MAX index,and retun old argument？
 */
public class ItemFuncGreatest extends ItemFuncMinMax {

    public ItemFuncGreatest(List<Item> args) {
        super(args, -1);
    }

    @Override
    public final String funcName() {
        return "greatest";
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncGreatest(realArgs);
    }
}
