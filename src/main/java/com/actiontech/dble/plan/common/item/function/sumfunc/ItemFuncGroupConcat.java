/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function.sumfunc;

import com.actiontech.dble.backend.mysql.nio.handler.util.RowDataComparator;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.context.NameResolutionContext;
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
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;


public class ItemFuncGroupConcat extends ItemSum {
    private StringBuilder resultSb;
    private String seperator;
    private List<Order> orders;
    private boolean alwaysNull; // if contains null
    private RowDataComparator rowComparator;
    private Queue<OrderResult> reusltList;
    public ItemFuncGroupConcat(List<Item> selItems, boolean distinct, List<Order> orders, String isSeparator,
                               boolean isPushDown, List<Field> fields, int charsetIndex) {
        super(selItems, isPushDown, fields, charsetIndex);
        seperator = isSeparator;
        this.resultSb = new StringBuilder();
        this.orders = orders;
        this.alwaysNull = false;
        setDistinct(distinct);
    }

    @Override
    public SumFuncType sumType() {
        return SumFuncType.GROUP_CONCAT_FUNC;
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
        if (reusltList != null) {
            reusltList.clear();
        }
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
        if (orders != null) {
            if (sourceFields != null && rowComparator == null) {
                rowComparator = new RowDataComparator(sourceFields, orders);
                reusltList = new PriorityQueue<>(11, new Comparator<OrderResult>() {
                    @Override
                    public int compare(OrderResult o1, OrderResult o2) {
                        RowDataPacket row1 = o1.row;
                        RowDataPacket row2 = o2.row;
                        if (row1 == null || row2 == null) {
                            if (row1 == row2)
                                return 0;
                            if (row1 == null)
                                return -1;
                            return 1;
                        }
                        return rowComparator.compare(row1, row2);
                    }
                });
            }
            reusltList.add(new OrderResult(rowStr.toString(), row));
        } else {
            if (resultSb.length() > 0)
                resultSb.append(seperator);
            resultSb.append(rowStr);
        }
        nullValue = false;
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
        if (orders != null) {
            for (OrderResult orderResult : reusltList) {
                if (resultSb.length() > 0)
                    resultSb.append(seperator);
                resultSb.append(orderResult.result);
            }
        }
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
                    false, null, charsetIndex);
        } else {
            return new ItemFuncGroupConcat(calArgs, hasWithDistinct(), this.orders, this.seperator, isPushDown,
                    fields, charsetIndex);
        }
    }

    public final void fixOrders(NameResolutionContext context) {
        if (orders == null) return;
        for (Order order : orders) {
            Item arg = order.getItem();
            Item fixedArg = arg.fixFields(context);
            if (fixedArg == null)
                return ;
            order.setItem(fixedArg);
            getReferTables().addAll(fixedArg.getReferTables());
        }
    }
    public List<Order> getOrders() {
        return orders;
    }

    private static class OrderResult {
        private String result;
        private RowDataPacket row;

        OrderResult(String result, RowDataPacket row) {
            this.result = result;
            this.row = row;

        }
    }
}
