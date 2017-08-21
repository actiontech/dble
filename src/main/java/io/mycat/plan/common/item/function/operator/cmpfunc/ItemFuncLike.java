package io.mycat.plan.common.item.function.operator.cmpfunc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.operator.ItemBoolFunc2;
import io.mycat.util.CompareLike;

import java.math.BigInteger;
import java.util.List;


public class ItemFuncLike extends ItemBoolFunc2 {
    private Item escape;
    private boolean isNot;

    public ItemFuncLike(Item a, Item b, Item escape, boolean isNot) {
        super(a, b);
        this.escape = escape;
        if (escape != null)
            args.add(escape);
        this.isNot = isNot;
    }

    @Override
    public final String funcName() {
        return isNot ? "not like " : "like";
    }

    public Functype functype() {
        return Functype.LIKE_FUNC;
    }

    @Override
    public BigInteger valInt() {
        String str = args.get(0).valStr();
        if (args.get(0).isNull()) {
            this.nullValue = true;
            return BigInteger.ZERO;
        }
        String str2 = args.get(1).valStr();
        if (args.get(1).isNull()) {
            this.nullValue = true;
            return BigInteger.ZERO;
        }
        String escapeStr = null;
        if (escape != null)
            escapeStr = escape.valStr();
        this.nullValue = false;
        CompareLike like = null;
        if (escapeStr == null)
            like = new CompareLike(str2);
        else
            like = new CompareLike(str2, escapeStr);
        boolean isLike = like.compare(str);
        return isNot ? (isLike ? BigInteger.ZERO : BigInteger.ONE) : (isLike ? BigInteger.ONE : BigInteger.ZERO);
    }

    @Override
    public SQLExpr toExpression() {
        SQLExpr comparee = args.get(0).toExpression();
        SQLExpr pattern = args.get(1).toExpression();
        SQLExpr escape = this.escape == null ? null : this.escape.toExpression();
        SQLBinaryOpExpr like = null;
        if (isNot) {
            like = new SQLBinaryOpExpr(comparee, SQLBinaryOperator.NotLike, pattern);
        } else {
            like = new SQLBinaryOpExpr(comparee, SQLBinaryOperator.Like, pattern);
        }
        if (escape == null) {
            return like;
        } else {
            return new SQLBinaryOpExpr(like, SQLBinaryOperator.Escape, escape);
        }
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncLike(newArgs.get(0), newArgs.get(1), escape == null ? null : newArgs.get(2), this.isNot);
    }

}
