package io.mycat.plan.common.item.function.operator.cmpfunc;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.ItemFunc;

import java.util.List;


/*
 * select least(c1,c2,'5') from t,结果为5
 * select least(c1,c2,'a') from t,结果为'a'
 * select least(c1,c2,'a')+1 from t,结果为1
 * 是否根据参数类型已经不能判断返回类型？
 * calculate时是否应该保存最大值的index号，然后返回旧的argument？
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
