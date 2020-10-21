/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.castfunc;

import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemStrFunc;
import com.alibaba.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;

import java.util.ArrayList;
import java.util.List;


public class ItemNCharTypeCast extends ItemStrFunc {
    private int castLength;

    public ItemNCharTypeCast(Item a, int lengthArg, int charsetIndex) {
        super(new ArrayList<Item>(), charsetIndex);
        args.add(a);
        this.castLength = lengthArg;
    }

    @Override
    public final String funcName() {
        return "cast_as_nchar";
    }

    @Override
    public void fixLengthAndDec() {
        fixCharLength(castLength >= 0 ? castLength : args.get(0).getMaxLength());
    }

    @Override
    public String valStr() {
        assert (fixed);

        String res = null;
        if ((res = args.get(0).valStr()) == null) {
            nullValue = true;
            return null;
        }
        nullValue = false;
        if (castLength != -1 && castLength < res.length())
            res = res.substring(0, castLength);
        return res;
    }

    @Override
    public SQLExpr toExpression() {
        SQLCastExpr cast = new SQLCastExpr();
        cast.setExpr(args.get(0).toExpression());
        SQLDataTypeImpl dataType = new SQLDataTypeImpl("NCAHR");
        if (castLength >= 0) {
            dataType.addArgument(new SQLIntegerExpr(castLength));
        }
        cast.setDataType(dataType);

        return cast;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        List<Item> newArgs = null;
        if (!forCalculate)
            newArgs = cloneStructList(args);
        else
            newArgs = calArgs;
        return new ItemNCharTypeCast(newArgs.get(0), castLength, charsetIndex);
    }
}
