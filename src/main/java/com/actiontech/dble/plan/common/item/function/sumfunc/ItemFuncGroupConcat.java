/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.item.FieldTypes;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFuncKeyWord;
import com.actiontech.dble.plan.common.time.MySQLTime;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateOption;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


public class ItemFuncGroupConcat extends ItemSum {
    protected StringBuilder resultSb;
    protected String seperator;
    private List<Order> orders;
    protected boolean alwaysNull; // if contains null

    public ItemFuncGroupConcat(List<Item> selItems, boolean distinct, List<Order> orders, String isSeparator,
                               boolean isPushDown, List<Field> fields) {
        super(selItems, isPushDown, fields);
        this.orders = orders;
        seperator = isSeparator;
        this.resultSb = new StringBuilder();
        this.alwaysNull = false;
        setDistinct(distinct);
    }

    @Override
    public Sumfunctype sumType() {
        return Sumfunctype.GROUP_CONCAT_FUNC;
    }

    @Override
    public String funcName() {
        return "GROUP_CONCAT";
    }

    @Override
    public ItemResult resultType() {
        return ItemResult.STRING_RESULT;
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_VARCHAR;
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

    @Override
    public void clear() {
        resultSb.setLength(0);
        nullValue = true;
    }

    @Override
    public Object getTransAggObj() {
        throw new RuntimeException("Group_concat should not use direct groupby!");
    }

    @Override
    public int getTransSize() {
        throw new RuntimeException("Group_concat should not use direct groupby!");
    }

    @Override
    public boolean add(RowDataPacket row, Object tranObject) {
        if (alwaysNull)
            return false;
        StringBuilder rowStr = new StringBuilder();
        for (int i = 0; i < getArgCount(); i++) {
            Item item = args.get(i);
            String s = item.valStr();
            if (item.isNull())
                return false;
            rowStr.append(s);
        }
        nullValue = false;
        if (resultSb.length() > 0)
            resultSb.append(seperator);
        resultSb.append(rowStr);
        return false;
    }

    @Override
    public boolean setup() {
        alwaysNull = false;
        for (int i = 0; i < getArgCount(); i++) {
            Item item = args.get(i);
            if (item.canValued()) {
                if (item.isNull()) {
                    alwaysNull = true;
                    return false;
                }
            }
        }
        return false;
    }

    @Override
    public boolean fixFields() {
        super.fixFields();
        nullValue = true;
        fixed = true;
        return false;
    }

    @Override
    public String valStr() {
        if (aggr != null)
            aggr.endup();
        if (nullValue)
            return null;
        return resultSb.toString();
    }

    @Override
    public BigDecimal valReal() {
        String res = valStr();
        if (res == null)
            return BigDecimal.ZERO;
        else
            try {
                return new BigDecimal(res);
            } catch (Exception e) {
                LOGGER.info("group_concat val_real() convert exception, string value is: " + res);
                return BigDecimal.ZERO;
            }
    }

    @Override
    public BigInteger valInt() {
        String res = valStr();
        if (res == null)
            return BigInteger.ZERO;
        else
            try {
                return new BigInteger(res);
            } catch (Exception e) {
                LOGGER.info("group_concat val_int() convert exception, string value is: " + res);
                return BigInteger.ZERO;
            }
    }

    @Override
    public BigDecimal valDecimal() {
        return valDecimalFromString();
    }

    @Override
    public boolean getDate(MySQLTime ltime, long fuzzydate) {
        return getDateFromString(ltime, fuzzydate);
    }

    @Override
    public boolean getTime(MySQLTime ltime) {
        return getTimeFromString(ltime);
    }

    @Override
    public boolean pushDownAdd(RowDataPacket row) {
        throw new RuntimeException("not implement");
    }

    @Override
    public SQLExpr toExpression() {
        SQLAggregateExpr aggregate = new SQLAggregateExpr(funcName());
        if (hasWithDistinct()) {
            aggregate.setOption(SQLAggregateOption.DISTINCT);
        }
        if (orders != null) {
            SQLOrderBy orderBy = new SQLOrderBy();
            for (Order order : orders) {
                SQLSelectOrderByItem orderItem = new SQLSelectOrderByItem(order.getItem().toExpression());
                orderItem.setType(order.getSortOrder());
                orderBy.addItem(orderItem);
            }
            aggregate.putAttribute(ItemFuncKeyWord.ORDER_BY, orderBy);
        }
        for (Item arg : args) {
            aggregate.addArgument(arg.toExpression());
        }
        if (seperator != null) {
            SQLCharExpr sep = new SQLCharExpr(seperator);
            aggregate.putAttribute(ItemFuncKeyWord.SEPARATOR, sep);
        }
        return aggregate;
    }

    @Override
    protected Item cloneStruct(boolean forCalculate, List<Item> calArgs, boolean isPushDown, List<Field> fields) {
        if (!forCalculate) {
            List<Item> argList = cloneStructList(args);
            return new ItemFuncGroupConcat(argList, hasWithDistinct(), this.orders, this.seperator,
                    false, null);
        } else {
            return new ItemFuncGroupConcat(calArgs, hasWithDistinct(), this.orders, this.seperator, isPushDown,
                    fields);
        }
    }

}
