/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.operator.cmpfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.operator.ItemBoolFunc2;
import com.actiontech.dble.util.CompareLike;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;

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

    public ItemFunc.Functype functype() {
        return ItemFunc.Functype.LIKE_FUNC;
    }

    @Override
    public BigInteger valInt() {
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

        String str = args.get(0).valStr();
        boolean isLike = like.compare(str);
        return isNot ? (isLike ? BigInteger.ZERO : BigInteger.ONE) : (isLike ? BigInteger.ONE : BigInteger.ZERO);
    }

    @Override
    public SQLExpr toExpression() {
        SQLExpr comparee = args.get(0).toExpression();
        SQLExpr pattern = args.get(1).toExpression();
        SQLExpr escapeExpr = this.escape == null ? null : this.escape.toExpression();
        SQLBinaryOpExpr like = null;
        if (isNot) {
            like = new SQLBinaryOpExpr(comparee, SQLBinaryOperator.NotLike, pattern);
        } else {
            like = new SQLBinaryOpExpr(comparee, SQLBinaryOperator.Like, pattern);
        }
        if (escapeExpr == null) {
            return like;
        } else {
            return new SQLBinaryOpExpr(like, SQLBinaryOperator.Escape, escapeExpr);
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
