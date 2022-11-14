/*
 * Copyright (C) 2016-2022 ActionTech.
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
import com.actiontech.dble.plan.optimizer.HintPlanInfo;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class ItemMultiColumnARowSubQuery extends ItemSubQuery {

    protected List<List<Item>> value = new ArrayList<>();
    protected List<Item> field = new ArrayList<>();
    protected List<Item> select;

    /**
     * @param currentDb
     * @param query
     */
    public ItemMultiColumnARowSubQuery(String currentDb, SQLSelectQuery query, ProxyMetaManager metaManager, Map<String, String> usrVariables, int charsetIndex, @Nullable HintPlanInfo hintPlanInfo) {
        super(currentDb, query, metaManager, usrVariables, charsetIndex, hintPlanInfo);
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

    public List<Item> getSelect() {
        return select;
    }

    public void setSelect(List<Item> select) {
        this.select = select;
    }

    public List<List<Item>> getValue() {
        return value;
    }

    public List<Item> getField() {
        return field;
    }

    public void setField(List<Item> field) {
        this.field = field;
    }

}
