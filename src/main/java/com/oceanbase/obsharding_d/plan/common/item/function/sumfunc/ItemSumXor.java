/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.sumfunc;

import com.oceanbase.obsharding_d.net.mysql.RowDataPacket;
import com.oceanbase.obsharding_d.plan.common.field.Field;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.math.BigInteger;
import java.util.List;


public class ItemSumXor extends ItemSumBit {

    public ItemSumXor(List<Item> itemPar, boolean isPushDown, List<Field> fields, int charsetIndex) {
        super(itemPar, 0, isPushDown, fields, charsetIndex);
    }

    @Override
    public boolean add(RowDataPacket row, Object transObj) {
        if (transObj != null) {
            AggData other = (AggData) transObj;
            if (!other.isNull)
                bits = bits.xor(other.bits);
        } else {
            BigInteger value = args.get(0).valInt();
            if (!args.get(0).isNullValue())
                bits = bits.xor(value);
        }
        return false;
    }

    /**
     * xor's push-down is xor
     */
    @Override
    public boolean pushDownAdd(RowDataPacket row) {
        BigInteger value = args.get(0).valInt();
        if (!args.get(0).isNullValue())
            bits = bits.xor(value);
        return false;
    }

    @Override
    public String funcName() {
        return "BIT_XOR";
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName());
        for (Item arg : args) {
            method.addArgument(arg.toExpression());
        }
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        if (!forCalculate) {
            List<Item> newArgs = cloneStructList(args);
            return new ItemSumXor(newArgs, false, null, charsetIndex);
        } else {
            return new ItemSumXor(calArgs, isPushDown, fields, charsetIndex);
        }
    }

}
