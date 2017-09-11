/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.castfunc;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFuncKeyWord;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemStrFunc;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ItemFuncConvCharset extends ItemStrFunc {
    private String mysqlCharset;
    private String javaCharset;

    public ItemFuncConvCharset(Item a, String charset) {
        super(a);
        mysqlCharset = charset;
        javaCharset = CharsetUtil.getJavaCharset(charset);
    }

    @Override
    public final String funcName() {
        return "CONVERT";
    }

    @Override
    public void fixLengthAndDec() {

    }

    @Override
    public String valStr() {
        String argVal = args.get(0).valStr();
        if (argVal == null) {
            nullValue = true;
            return null;
        }
        try {
            return new String(argVal.getBytes(), javaCharset);
        } catch (UnsupportedEncodingException e) {
            LOGGER.warn("convert using charset exception", e);
            nullValue = true;
            return null;
        }
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr(funcName());
        method.addParameter(args.get(0).toExpression());
        method.putAttribute(ItemFuncKeyWord.USING, mysqlCharset);
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemFuncConvCharset(newArgs.get(0), mysqlCharset);
    }

}
