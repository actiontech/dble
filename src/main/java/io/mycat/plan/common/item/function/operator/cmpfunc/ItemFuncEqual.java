package io.mycat.plan.common.item.function.operator.cmpfunc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.operator.ItemBoolFunc2;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncEqual extends ItemBoolFunc2 {

    public ItemFuncEqual(Item a, Item b) {
        super(a, b);
    }

    @Override
    public final String funcName() {
        return "=";
    }

    @Override
    public Functype functype() {
        return Functype.EQ_FUNC;
    }

    @Override
    public BigInteger valInt() {
        int value = cmp.compare();
        return value == 0 ? BigInteger.ONE : BigInteger.ZERO;
    }

    @Override
    public SQLExpr toExpression() {
        SQLExpr left = args.get(0).toExpression();
        SQLExpr right = args.get(1).toExpression();
        return new SQLBinaryOpExpr(left, SQLBinaryOperator.Equality, right);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncEqual(newArgs.get(0), newArgs.get(1));
    }

}
