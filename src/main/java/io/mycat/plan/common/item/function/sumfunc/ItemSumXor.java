package io.mycat.plan.common.item.function.sumfunc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;

import java.math.BigInteger;
import java.util.List;


public class ItemSumXor extends ItemSumBit {

    public ItemSumXor(List<Item> itemPar, boolean isPushDown, List<Field> fields) {
        super(itemPar, 0, isPushDown, fields);
    }

    @Override
    public boolean add(RowDataPacket row, Object transObj) {
        if (transObj != null) {
            AggData other = (AggData) transObj;
            if (!other.isNull)
                bits = bits.xor(other.bits);
        } else {
            BigInteger value = args.get(0).valInt();
            if (!args.get(0).nullValue)
                bits = bits.xor(value);
        }
        return false;
    }

    /**
     * xor的pushdown为xor
     */
    @Override
    public boolean pushDownAdd(RowDataPacket row) {
        BigInteger value = args.get(0).valInt();
        if (!args.get(0).nullValue)
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
            method.addParameter(arg.toExpression());
        }
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        if (!forCalculate) {
            List<Item> newArgs = cloneStructList(args);
            return new ItemSumXor(newArgs, false, null);
        } else {
            return new ItemSumXor(calArgs, isPushDown, fields);
        }
    }

}
