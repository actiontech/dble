package io.mycat.plan.common.item.function.mathsfunc.operator;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.primary.ItemNumOp;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemFuncMod extends ItemNumOp {

    public ItemFuncMod(Item a, Item b) {
        super(a, b);
    }

    @Override
    public final String funcName() {
        return "%";
    }

    @Override
    public void fixLengthAndDec() {
        super.fixLengthAndDec();
        maybeNull = true;
    }

    @Override
    public BigDecimal realOp() {
        BigDecimal val0 = args.get(0).valReal();
        BigDecimal val1 = args.get(1).valReal();
        if ((this.nullValue = args.get(0).isNull() || args.get(1).isNull()))
            return BigDecimal.ZERO;
        if (val1.compareTo(BigDecimal.ZERO) == 0) {
            signalDivideByNull();
            return BigDecimal.ZERO;
        }
        BigInteger tmp = val0.toBigInteger().divide(val1.toBigInteger());
        BigDecimal tmpBd = new BigDecimal(tmp.multiply(val1.toBigInteger()));
        return val0.subtract(tmpBd);

    }

    @Override
    public BigInteger intOp() {
        if (this.nullValue)
            return BigInteger.ZERO;
        BigInteger v0 = args.get(0).valInt();
        BigInteger v1 = args.get(1).valInt();
        if (v1.equals(BigInteger.ZERO)) {
            signalDivideByNull();
            return BigInteger.ZERO;
        }
        return v0.divide(v1);
    }

    @Override
    public BigDecimal decimalOp() {
        if (this.nullValue)
            return new BigDecimal(0);
        BigDecimal val0 = args.get(0).valDecimal();
        BigDecimal val1 = args.get(1).valDecimal();
        if (val1.compareTo(BigDecimal.ZERO) == 0) {
            signalDivideByNull();
            return null;
        }
        BigInteger tmp = val0.toBigInteger().divide(val0.toBigInteger());
        BigDecimal tmpBd = new BigDecimal(tmp.multiply(val0.toBigInteger()));
        return val0.subtract(tmpBd);
    }

    @Override
    public void resultPrecision() {
        decimals = Math.max(args.get(0).decimals, args.get(1).decimals);
        maxLength = Math.max(args.get(0).maxLength, args.get(1).maxLength);
    }

    @Override
    public SQLExpr toExpression() {
        return new SQLBinaryOpExpr(args.get(0).toExpression(), SQLBinaryOperator.Mod, args.get(1).toExpression());
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncMod(newArgs.get(0), newArgs.get(1));
    }

}
