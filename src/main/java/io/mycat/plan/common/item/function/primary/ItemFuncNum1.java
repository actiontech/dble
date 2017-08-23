package io.mycat.plan.common.item.function.primary;

import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.time.MySQLTime;

import java.util.List;


/**
 * function where type of result detected by first argument
 */
public abstract class ItemFuncNum1 extends ItemFuncNumhybrid {

    public ItemFuncNum1(List<Item> args) {
        super(args);
    }

    @Override
    public void fixNumLengthAndDec() {
        decimals = args.get(0).decimals;
        this.maxLength = args.get(0).maxLength;
    }

    @Override
    public void findNumType() {
        ItemResult i = hybridType = args.get(0).resultType();
        if (i == ItemResult.INT_RESULT) {
        } else if (i == ItemResult.STRING_RESULT || i == ItemResult.REAL_RESULT) {
            hybridType = ItemResult.REAL_RESULT;
            maxLength = floatLength(decimals);

        } else if (i == ItemResult.DECIMAL_RESULT) {
        } else {
            assert (false);
        }
    }

    @Override
    public String strOp() {
        return null;
    }

    @Override
    public boolean dateOp(MySQLTime ltime, long flags) {
        return false;
    }

    @Override
    public boolean timeOp(MySQLTime ltime) {
        return false;
    }
}
