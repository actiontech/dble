/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.util.ArgComparator;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;


public class ItemFuncIn extends ItemFuncOptNeg {
    //TODO :DELETE
    private ItemResult leftResultType;

    /**
     * select 'a' in ('a','b','c') args(0) is 'a',[1] is 'b',[2] is'c'...
     *
     * @param args
     */
    public ItemFuncIn(List<Item> args, boolean isNegation) {
        super(args, isNegation);
    }

    @Override
    public final String funcName() {
        return "in";
    }

    @Override
    public void fixLengthAndDec() {
        for (int i = 1; i < args.size(); i++) {
            args.get(i).setCmpContext(MySQLcom.itemCmpType(leftResultType, args.get(i).resultType()));
        }
        maxLength = 1;
    }

    @Override
    public BigInteger valInt() {
        if ((nullValue = args.get(0).type() == Item.ItemType.NULL_ITEM))
            return BigInteger.ZERO;
        Item left = args.get(0);
        if (nullValue = left.type() == ItemType.NULL_ITEM) {
            return BigInteger.ZERO;
        }
        boolean haveNull = false;
        for (int i = 1; i < args.size(); i++) {
            Item right = args.get(i);
            if (right.type() == ItemType.NULL_ITEM) {
                haveNull = true;
                continue;
            }
            if (nullValue = left.isNullValue())
                return BigInteger.ZERO;
            ArgComparator cmp = new ArgComparator(left, right);
            cmp.setCmpFunc(this, left, right, false);
            if (cmp.compare() == 0 && !right.isNullValue())
                return !negated ? BigInteger.ONE : BigInteger.ZERO;
            haveNull |= right.isNull();
        }
        nullValue = haveNull;
        return (!nullValue && negated) ? BigInteger.ONE : BigInteger.ZERO;
    }

    @Override
    public SQLExpr toExpression() {
        SQLInListExpr in = new SQLInListExpr(args.get(0).toExpression(), this.negated);
        List<SQLExpr> targetList = new ArrayList<>();
        int index = 0;
        for (Item item : args) {
            if (index != 0) {
                targetList.add(item.toExpression());
            }
            index++;
        }
        in.setTargetList(targetList);
        return in;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncIn(newArgs, this.negated);
    }

}
