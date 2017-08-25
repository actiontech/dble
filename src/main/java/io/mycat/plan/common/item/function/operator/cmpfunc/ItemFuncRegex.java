package io.mycat.plan.common.item.function.operator.cmpfunc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.primary.ItemBoolFunc;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncRegex extends ItemBoolFunc {

    public ItemFuncRegex(Item a, Item b) {
        super(a, b);
    }

    @Override
    public final String funcName() {
        return "regexp";
    }

    @Override
    public BigInteger valInt() {
        String arg0 = args.get(0).valStr();
        String arg1 = args.get(1).valStr();
        if (nullValue = (args.get(0).isNullValue() || args.get(1).isNullValue()))
            return BigInteger.ZERO;
        return arg0.matches(arg1) ? BigInteger.ONE : BigInteger.ZERO;
    }

    @Override
    public SQLExpr toExpression() {
        SQLExpr left = args.get(0).toExpression();
        SQLExpr right = args.get(1).toExpression();
        return new SQLBinaryOpExpr(left, SQLBinaryOperator.RegExp, right);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncRegex(newArgs.get(0), newArgs.get(1));
    }
}
