/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.plan.common.item.subquery;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class ItemMultiRowSubQuery extends ItemSubQuery {

    protected boolean containNull = false;
    protected List<Item> value = new ArrayList<>();
    protected Item filed;
    protected Item select;
    /**
     * @param currentDb
     * @param query
     */
    public ItemMultiRowSubQuery(String currentDb, SQLSelectQuery query, ProxyMetaManager metaManager, Map<String, String> usrVariables, int charsetIndex) {
        super(currentDb, query, metaManager, usrVariables, charsetIndex);
    }

    @Override
    public void fixLengthAndDec() {
    }

    @Override
    public BigDecimal valReal() {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
    }

    @Override
    public BigInteger valInt() {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
    }

    @Override
    public String valStr() {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
    }

    @Override
    public BigDecimal valDecimal() {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not support yet!");
    }



    public Item getSelect() {
        return select;
    }

    public void setSelect(Item select) {
        this.select = select;
    }

    public Item getFiled() {
        return filed;
    }

    public void setFiled(Item filed) {
        this.filed = filed;
    }

    public List<Item> getValue() {
        return value;
    }


    public boolean isContainNull() {
        return containNull;
    }

    public void setContainNull(boolean containNull) {
        this.containNull = containNull;
    }
}
