package io.mycat.plan.common.item.function.sumfunc;

import java.math.BigInteger;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;


public class ItemSumAnd extends ItemSumBit {
    public ItemSumAnd(List<Item> itemPar, boolean isPushDown, List<Field> fields) {
        super(itemPar, -1, isPushDown, fields);
    }

    @Override
    public boolean add(RowDataPacket row, Object transObj) {
        if (transObj != null) {
            AggData other = (AggData) transObj;
            if (!other.isNull)
                bits = bits.and(other.bits);
        } else {
            BigInteger value = args.get(0).valInt();
            if (!args.get(0).isNullValue())
                bits = bits.and(value);
        }
        return false;
    }

    /**
     * add's push-down is add
     */
    @Override
    public boolean pushDownAdd(RowDataPacket row) {
        BigInteger value = args.get(0).valInt();
        if (!args.get(0).isNullValue())
            bits = bits.and(value);
        return false;
    }

    @Override
    public String funcName() {
        return "BIT_AND";
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName());
        for (Item arg : args) {
            method.addParameter(arg.toExpression());
        }
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        if (!forCalculate) {
            List<Item> newArgs = cloneStructList(args);
            return new ItemSumAnd(newArgs, false, null);
        } else {
            return new ItemSumAnd(calArgs, isPushDown, fields);
        }
    }

}
