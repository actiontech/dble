/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.util;

import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.MysqlVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler.HandlerType;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.field.Field;
import com.actiontech.dble.plan.common.field.FieldUtil;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.ItemInt;
import com.actiontech.dble.plan.common.item.ItemRef;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class HandlerTool {
    private HandlerTool() {
    }

    // private static Pattern pat = Pattern.compile("^\'([^\']*?)\'$");

    /**
     * @param node
     */
    public static void terminateHandlerTree(final DMLResponseHandler node) {
        try {
            if (node == null)
                return;
            Set<DMLResponseHandler> merges = node.getMerges();
            for (DMLResponseHandler merge : merges) {
                DMLResponseHandler currentHandler = merge;
                while (currentHandler != node) {
                    currentHandler.terminate();
                    currentHandler = currentHandler.getNextHandler();
                }
            }
            node.terminate();
        } catch (Exception e) {
            Logger.getLogger(HandlerTool.class).error("terminate node exception:", e);
        }
    }

    public static Field createField(FieldPacket fp) {
        Field field = Field.getFieldItem(fp.getName(), fp.getTable(), fp.getType(), fp.getCharsetIndex(), (int) fp.getLength(), fp.getDecimals(),
                fp.getFlags());
        return field;
    }

    public static List<Field> createFields(List<FieldPacket> fps) {
        List<Field> ret = new ArrayList<>();
        for (FieldPacket fp : fps) {
            Field field = createField(fp);
            ret.add(field);
        }
        return ret;
    }

    /**
     * create Item, the Item value referenced by field and changed by field changes
     *
     * @param sel
     * @param fields
     * @param type
     * @return
     */
    public static Item createItem(Item sel, List<Field> fields, int startIndex, boolean allPushDown, HandlerType type) {
        Item ret;
        if (sel.basicConstItem())
            return sel;
        Item.ItemType i = sel.type();
        if (i == Item.ItemType.FUNC_ITEM || i == Item.ItemType.COND_ITEM) {
            ItemFunc func = (ItemFunc) sel;
            if (func.getPushDownName() == null || func.getPushDownName().length() == 0) {
                ret = createFunctionItem(func, fields, startIndex, allPushDown, type);
            } else {
                ret = createFieldItem(func, fields, startIndex);
            }

        } else if (i == Item.ItemType.SUM_FUNC_ITEM) {
            ItemSum sumFunc = (ItemSum) sel;
            if (type != HandlerType.GROUPBY) {
                ret = createFieldItem(sumFunc, fields, startIndex);
            } else if (sumFunc.getPushDownName() == null || sumFunc.getPushDownName().length() == 0) {
                ret = createSumItem(sumFunc, fields, startIndex, allPushDown, type);
            } else {
                ret = createPushDownGroupBy(sumFunc, fields, startIndex);
            }

        } else {
            ret = createFieldItem(sel, fields, startIndex);
        }
        ret.fixFields();
        return ret;
    }

    public static Item createRefItem(Item ref, String tbAlias, String fieldAlias) {
        return new ItemRef(ref, tbAlias, fieldAlias);
    }

    /**
     * clone field
     *
     * @param fields
     * @param bs
     */
    public static void initFields(List<Field> fields, List<byte[]> bs) {
        FieldUtil.initFields(fields, bs);
    }

    public static List<byte[]> getItemListBytes(List<Item> items) {
        List<byte[]> ret = new ArrayList<>(items.size());
        for (Item item : items) {
            byte[] b = item.getRowPacketByte();
            ret.add(b);
        }
        return ret;
    }

    public static ItemField createItemField(FieldPacket fp) {
        Field field = createField(fp);
        return new ItemField(field);
    }

    // ------------------------------- helper methods ------------------------

    /**
     * 1.count(id) : count(id) = sum[count(id) 0...n];
     * 2.sum(id): sum(id) = sum[sum(id) 0...n];
     * 3.avg(id) avg(id) = sum[sum(id) 0...n]/sum[count(id) 0...n];
     *
     * @param sumfun aggregate function name
     * @param fields
     * @return
     */
    protected static Item createPushDownGroupBy(ItemSum sumfun, List<Field> fields, int startIndex) {
        String funName = sumfun.funcName().toUpperCase();
        String colName = sumfun.getItemName();
        String pdName = sumfun.getPushDownName();
        Item ret = null;
        List<Item> args = new ArrayList<>();
        if (funName.equalsIgnoreCase("AVG")) {
            String colNameSum = colName.replace(funName + "(", "SUM(");
            String colNameCount = colName.replace(funName + "(", "COUNT(");
            Item sumfunSum = new ItemField(null, null, colNameSum);
            sumfunSum.setPushDownName(
                    pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("SUM")));
            Item sumfunCount = new ItemField(null, null, colNameCount);
            sumfunCount.setPushDownName(
                    pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("COUNT")));
            Item itemSum = createFieldItem(sumfunSum, fields, startIndex);
            Item itemCount = createFieldItem(sumfunCount, fields, startIndex);
            args.add(itemSum);
            args.add(itemCount);
        } else if (funName.equalsIgnoreCase("STD") || funName.equalsIgnoreCase("STDDEV_POP") ||
                funName.equalsIgnoreCase("STDDEV_SAMP") || funName.equalsIgnoreCase("STDDEV") ||
                funName.equalsIgnoreCase("VAR_POP") || funName.equalsIgnoreCase("VAR_SAMP") ||
                funName.equalsIgnoreCase("VARIANCE")) {
            // variance: v[0]:count,v[1]:sum,v[2]:variance(locally)
            String colNameCount = colName.replace(funName + "(", "COUNT(");
            String colNameSum = colName.replace(funName + "(", "SUM(");
            String colNameVar = colName.replace(funName + "(", "VARIANCE(");
            Item sumfunCount = new ItemField(null, null, colNameCount);
            sumfunCount.setPushDownName(
                    pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("COUNT")));
            Item sumfunSum = new ItemField(null, null, colNameSum);
            sumfunSum.setPushDownName(
                    pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("SUM")));
            Item sumfunVar = new ItemField(null, null, colNameVar);
            sumfunVar.setPushDownName(
                    pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("VARIANCE")));
            Item itemCount = createFieldItem(sumfunCount, fields, startIndex);
            Item itemSum = createFieldItem(sumfunSum, fields, startIndex);
            Item itemVar = createFieldItem(sumfunVar, fields, startIndex);
            args.add(itemCount);
            args.add(itemSum);
            args.add(itemVar);
        } else {
            Item subItem = createFieldItem(sumfun, fields, startIndex);
            args.add(subItem);
        }
        ret = sumfun.reStruct(args, true, fields);
        ret.setItemName(sumfun.getPushDownName() == null ? sumfun.getItemName() : sumfun.getPushDownName());
        return ret;
    }

    protected static ItemFunc createFunctionItem(ItemFunc f, List<Field> fields, int startIndex, boolean allPushDown,
                                                 HandlerType type) {
        ItemFunc ret = null;
        List<Item> args = new ArrayList<>();
        for (int index = 0; index < f.getArgCount(); index++) {
            Item arg = f.arguments().get(index);
            Item newArg = null;
            if (arg.isWild())
                newArg = new ItemInt(0);
            else
                newArg = createItem(arg, fields, startIndex, allPushDown, type);
            if (newArg == null)
                throw new RuntimeException("Function argument not found:" + arg);
            args.add(newArg);
        }
        ret = (ItemFunc) f.reStruct(args, allPushDown, fields);
        ret.setItemName(f.getPushDownName() == null ? f.getItemName() : f.getPushDownName());
        return ret;
    }

    /**
     * @param f
     * @param fields
     * @param startIndex
     * @param allPushDown
     * @param type
     * @return
     */
    private static ItemSum createSumItem(ItemSum f, List<Field> fields, int startIndex, boolean allPushDown,
                                         HandlerType type) {
        ItemSum ret = null;
        List<Item> args = new ArrayList<>();
        for (int index = 0; index < f.getArgCount(); index++) {
            Item arg = f.arguments().get(index);
            Item newArg = null;
            if (arg.isWild())
                newArg = new ItemInt(0);
            else
                newArg = createItem(arg, fields, startIndex, allPushDown, type);
            if (newArg == null)
                throw new RuntimeException("Function argument not found:" + arg);
            args.add(newArg);
        }
        ret = (ItemSum) f.reStruct(args, allPushDown, fields);
        ret.setItemName(f.getPushDownName() == null ? f.getItemName() : f.getPushDownName());
        return ret;
    }

    protected static ItemField createFieldItem(Item col, List<Field> fields, int startIndex) {
        int index = findField(col, fields, startIndex);
        if (index < 0)
            throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "field not found:" + col);
        ItemField ret = new ItemField(fields.get(index));
        ret.setItemName(col.getPushDownName() == null ? col.getItemName() : col.getPushDownName());
        return ret;
    }

    /**
     * findField in sel from start
     *
     * @param sel
     * @param fields
     * @param startIndex
     * @return
     */
    public static int findField(Item sel, List<Field> fields, int startIndex) {
        String selName = (sel.getPushDownName() == null ? sel.getItemName() : sel.getPushDownName());
        selName = selName.trim();
        String tableName = sel.getTableName();
        for (int index = startIndex; index < fields.size(); index++) {
            Field field = fields.get(index);
            // field.name==null if '' push down
            String colName2 = field.getName() == null ? null : field.getName().trim();
            String tableName2 = field.getTable();
            if (sel instanceof ItemField && !StringUtil.equals(tableName, tableName2)) {
                continue;
            }
            if (selName.equalsIgnoreCase(colName2)) {
                return index;
            }
        }
        return -1;
    }

    /**
     * make order by from distinct
     *
     * @param sels
     * @return
     */
    public static List<Order> makeOrder(List<Item> sels) {
        List<Order> orders = new ArrayList<>();
        for (Item sel : sels) {
            Order order = new Order(sel, SQLOrderingSpecification.ASC);
            orders.add(order);
        }
        return orders;
    }

    // @bug 1086
    public static boolean needSendNoRow(List<Order> groupBys) {
        return groupBys == null || groupBys.size() == 0;
    }
}
