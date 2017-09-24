/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;

import java.util.List;


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
