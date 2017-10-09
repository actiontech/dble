/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.subquery;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public abstract class ItemSingleRowSubQuery extends ItemSubQuery {
    protected Item value;
    protected Item select;
    protected boolean isField;

    public ItemSingleRowSubQuery(String currentDb, SQLSelectQuery query, boolean isField) {
        super(currentDb, query);
        this.select = this.planNode.getColumnsSelected().get(0);
        this.isField = isField;
    }


    @Override
    public void reset() {
        this.nullValue = true;
        if (value != null)
            value.setNullValue(true);
    }

    @Override
    public BigDecimal valReal() {
        if (!execute() && !value.isNullValue()) {
            nullValue = false;
            return value.valReal();
        } else {
            reset();
            return BigDecimal.ZERO;
        }
    }

    @Override
    public BigInteger valInt() {
        if (!execute() && !value.isNullValue()) {
            nullValue = false;
            return value.valInt();
        } else {
            reset();
            return BigInteger.ZERO;
        }
    }

    @Override
    public String valStr() {
        if (!execute() && !value.isNullValue()) {
            nullValue = false;
            return value.valStr();
        } else {
            reset();
            return null;
        }
    }

    @Override
    public BigDecimal valDecimal() {
        if (!execute() && !value.isNullValue()) {
            nullValue = false;
            return value.valDecimal();
        } else {
            reset();
            return null;
        }
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        if (!execute() && !value.isNullValue()) {
            nullValue = false;
            return value.getDate(ltime, fuzzydate);
        } else {
            reset();
            return true;
        }
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        if (!execute() && !value.isNullValue()) {
            nullValue = false;
            return value.getTime(ltime);
        } else {
            reset();
            return true;
        }
    }

    @Override
    public boolean valBool() {
        if (!execute() && !value.isNullValue()) {
            nullValue = false;
            return value.valBool();
        } else {
            reset();
            return false;
        }
    }

    @Override
    public void fixLengthAndDec() {

    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fieldList) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "unexpected!");
    }

    /*--------------------------------------getter/setter-----------------------------------*/

    public Item getValue() {
        return value;
    }

    public void setValue(Item value) {
        this.value = value;
    }

    public Item getSelect() {
        return select;
    }

    public void setSelect(Item select) {
        this.select = select;
    }

    public boolean isField() {
        return isField;
    }

    public void setField(boolean field) {
        isField = field;
    }

}
