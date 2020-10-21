/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.plan.common.item.subquery;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class ItemExistsSubQuery extends ItemSingleRowSubQuery {
    private boolean isNot;
    public ItemExistsSubQuery(String currentDb, SQLSelectQuery query, boolean isNot, ProxyMetaManager metaManager, Map<String, String> usrVariables, int charsetIndex) {
        super(currentDb, query, false, metaManager, usrVariables, charsetIndex);
        this.isNot = isNot;
        if (!this.correlatedSubQuery) {
            if ((this.planNode.getLimitFrom() == -1)) {
                this.planNode.setLimitFrom(0);
                this.planNode.setLimitTo(1);
            } else if (this.planNode.getLimitTo() > 1) {
                this.planNode.setLimitTo(1);
            }
            this.select = new ItemInt(1L);
            this.planNode.getColumnsSelected().add(select);
        }
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

    @Override
    public SQLExpr toExpression() {
        return new SQLExistsExpr(new SQLSelect(query), isNot);
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        return new ItemExistsSubQuery(this.currentDb, this.query, this.isNot, this.metaManager, this.usrVariables, this.charsetIndex);
    }
    @Override
    public SubSelectType subType() {
        return SubSelectType.EXISTS_SUBS;
    }

    public boolean isNot() {
        return isNot;
    }

}
