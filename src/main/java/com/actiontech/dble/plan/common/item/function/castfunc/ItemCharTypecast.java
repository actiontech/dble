/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.castfunc;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemStrFunc;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;


public class ItemCharTypecast extends ItemStrFunc {
    private int castLength;
    private String charSetName;

    public ItemCharTypecast(Item a, int lengthArg, String charSetName) {
        super(new ArrayList<Item>());
        args.add(a);
        this.castLength = lengthArg;
        this.charSetName = charSetName;
    }

    @Override
    public final String funcName() {
        return "cast_as_char";
    }

    @Override
    public void fixLengthAndDec() {
        fixCharLength(castLength >= 0 ? castLength : args.get(0).getMaxLength());
    }

    @Override
    public String valStr() {
        assert (fixed && castLength >= 0);

        String res = null;
        if ((res = args.get(0).valStr()) == null) {
            nullValue = true;
            return null;
        }
        nullValue = false;
        if (castLength < res.length()) {
            res = res.substring(0, castLength);
        }
        if (charSetName != null) {
            try {
                res = new String(res.getBytes(), CharsetUtil.getJavaCharset(charSetName));
            } catch (UnsupportedEncodingException e) {
                Item.LOGGER.warn("convert using charset exception", e);
                nullValue = true;
                return null;
            }
        }
        return res;
    }

    @Override
    public SQLExpr toExpression() {
        SQLCastExpr cast = new SQLCastExpr();
        cast.setExpr(args.get(0).toExpression());
        SQLCharacterDataType dataType = new SQLCharacterDataType(SQLCharacterDataType.CHAR_TYPE_CHAR);
        cast.setDataType(dataType);
        if (castLength >= 0) {
            dataType.addArgument(new SQLIntegerExpr(castLength));
        }
        if (charSetName != null) {
            dataType.setName(charSetName);
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
        return new ItemCharTypecast(newArgs.get(0), castLength, charSetName);
    }
}
