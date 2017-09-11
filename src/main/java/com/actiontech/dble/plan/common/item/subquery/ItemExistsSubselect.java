/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.plan.common.item.subquery;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class ItemExistsSubselect extends ItemSubselect {

    /**
     * @param currentDb
     * @param query
     */
    public ItemExistsSubselect(String currentDb, SQLSelectQuery query, boolean isNot) {
        super(currentDb, query);
        boolean isNot1 = isNot;
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support!");
    }

    @Override
    public void fixLengthAndDec() {
        // TODO Auto-generated method stub

    }

    @Override
    public BigDecimal valReal() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BigInteger valInt() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String valStr() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BigDecimal valDecimal() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public SQLExpr toExpression() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        // TODO Auto-generated method stub
        return null;
    }

}
