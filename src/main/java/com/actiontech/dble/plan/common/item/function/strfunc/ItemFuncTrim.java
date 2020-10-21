/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.apache.commons.lang.StringUtils;

import java.util.List;


public class ItemFuncTrim extends ItemStrFunc {
    public enum TrimTypeEnum {
        DEFAULT, BOTH, LEADING, TRAILING, LTRIM, RTRIM
    }

    private TrimTypeEnum mTrimMode;
    private boolean mTrimLeading;
    private boolean mTrimTrailing;

    public ItemFuncTrim(Item a, Item b, TrimTypeEnum tm, int charsetIndex) {
        super(a, b, charsetIndex);
        this.mTrimMode = tm;
        mTrimLeading = trimLeading();
        mTrimTrailing = trimTrailing();
    }

    public ItemFuncTrim(Item a, TrimTypeEnum tm, int charsetIndex) {
        super(a, charsetIndex);
        this.mTrimMode = tm;
        mTrimLeading = trimLeading();
        mTrimTrailing = trimTrailing();
    }

    private boolean trimLeading() {
        return mTrimMode == TrimTypeEnum.DEFAULT || mTrimMode == TrimTypeEnum.BOTH || mTrimMode == TrimTypeEnum.LEADING ||
                mTrimMode == TrimTypeEnum.LTRIM;
    }

    private boolean trimTrailing() {
        return mTrimMode == TrimTypeEnum.DEFAULT || mTrimMode == TrimTypeEnum.BOTH || mTrimMode == TrimTypeEnum.TRAILING ||
                mTrimMode == TrimTypeEnum.RTRIM;
    }

    @Override
    public final String funcName() {
        if (mTrimMode == TrimTypeEnum.DEFAULT) {
            return "TRIM";
        } else if (mTrimMode == TrimTypeEnum.BOTH) {
            return "TRIM";
        } else if (mTrimMode == TrimTypeEnum.LEADING) {
            return "LTRIM";
        } else if (mTrimMode == TrimTypeEnum.TRAILING) {
            return "RTRIM";
        } else if (mTrimMode == TrimTypeEnum.LTRIM) {
            return "LTRIM";
        } else if (mTrimMode == TrimTypeEnum.RTRIM) {
            return "RTRIM";
        }
        return null;
    }

    @Override
    public void fixLengthAndDec() {

    }

    @Override
    public String valStr() {
        String toTrim = args.get(0).valStr();
        if (nullValue = args.get(0).isNullValue())
            return null;
        String remove = null;
        if (getArgCount() == 2) {
            remove = args.get(1).valStr();
            if (nullValue = args.get(1).isNullValue())
                return null;
        }
        String ret = null;
        if (mTrimLeading)
            ret = StringUtils.stripStart(toTrim, remove);
        if (mTrimTrailing)
            ret = StringUtils.stripEnd(toTrim, remove);
        return ret;
    }

    @Override
    public SQLExpr toExpression() {
        SQLMethodInvokeExpr method = new SQLMethodInvokeExpr();
        if (mTrimMode == TrimTypeEnum.LTRIM) {
            method.setMethodName("LTRIM");
            method.addParameter(args.get(0).toExpression());

        } else if (mTrimMode == TrimTypeEnum.RTRIM) {
            method.setMethodName("RTRIM");
            method.addParameter(args.get(0).toExpression());

        } else {
            method.setMethodName("TRIM");
            method.addParameter(args.get(0).toExpression());
            if (this.getArgCount() > 1) {
                method.setFrom(args.get(1).toExpression());
            }
            if (mTrimMode != TrimTypeEnum.DEFAULT) {
                method.setTrimOption(mTrimMode.toString());
            }
        }
        return method;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        if (getArgCount() == 2)
            return new ItemFuncTrim(newArgs.get(0), newArgs.get(1), mTrimMode, charsetIndex);
        else
            return new ItemFuncTrim(newArgs.get(0), mTrimMode, charsetIndex);
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        if (getArgCount() == 2)
            return new ItemFuncTrim(realArgs.get(0), realArgs.get(1), mTrimMode, charsetIndex);
        else
            return new ItemFuncTrim(realArgs.get(0), mTrimMode, charsetIndex);
    }

}
