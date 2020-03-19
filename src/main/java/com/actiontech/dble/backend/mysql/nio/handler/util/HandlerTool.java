/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.util;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor.MysqlVisitor;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler.HandlerType;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
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
import com.actiontech.dble.plan.common.item.function.ItemFuncInner;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemFuncGroupConcat;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class HandlerTool {
    private HandlerTool() {
    }

    // private static Pattern pat = Pattern.compile("^\'([^\']*?)\'$");

    /**
     * @param node DMLResponseHandler
     */
    public static void terminateHandlerTree(final DMLResponseHandler node) {
        try {
            if (node == null)
                return;
            List<DMLResponseHandler> merges = node.getMerges();
            for (DMLResponseHandler merge : merges) {
                DMLResponseHandler currentHandler = merge;
                while (currentHandler != node) {
                    currentHandler.terminate();
                    currentHandler = currentHandler.getNextHandler();
                }
            }
            node.terminate();
        } catch (Exception e) {
            LoggerFactory.getLogger(HandlerTool.class).error("terminate node exception:", e);
        }
    }

    public static Field createField(FieldPacket fp) {
        return Field.getFieldItem(fp.getName(), fp.getDb(), fp.getTable(), fp.getOrgTable(), fp.getType(),
                fp.getCharsetIndex(), (int) fp.getLength(), fp.getDecimals(), fp.getFlags());
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
     * @param sel    Item
     * @param fields List<Field>
     * @param type   HandlerType
     * @return Item
     */
    public static Item createItem(Item sel, List<Field> fields, int startIndex, boolean allPushDown, HandlerType type) {
        Item ret;
        if (sel.isWithSubQuery()) {
            sel = PlanUtil.rebuildSubQueryItem(sel);
        }
        if (sel.basicConstItem())
            return sel;
        Item.ItemType i = sel.type();
        if ((i == Item.ItemType.FUNC_ITEM || i == Item.ItemType.COND_ITEM) && !(sel instanceof ItemFuncInner)) {
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

    public static Item createRefItem(Item ref, String schema, String table, String tbAlias, String fieldAlias) {
        return new ItemRef(ref, schema, table, tbAlias, fieldAlias);
    }

    /**
     * clone field
     *
     * @param fields List<Field>
     * @param bs     List<byte[]>
     */
    public static void initFields(List<Field> fields, List<byte[]> bs) {
        FieldUtil.initFields(fields, bs);
    }

    static List<byte[]> getItemListBytes(List<Item> items) {
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
     * @param sumFunction aggregate function name
     * @param fields      List<Field>
     * @return Item
     */
    private static Item createPushDownGroupBy(ItemSum sumFunction, List<Field> fields, int startIndex) {
        String funName = sumFunction.funcName().toUpperCase();
        String colName = sumFunction.getItemName();
        String pdName = sumFunction.getPushDownName();
        Item ret;
        List<Item> args = new ArrayList<>();
        if (funName.equalsIgnoreCase("AVG")) {
            String colNameSum = colName.replace(funName + "(", "SUM(");
            String colNameCount = colName.replace(funName + "(", "COUNT(");
            Item sumFuncSum = new ItemField(null, null, colNameSum);
            sumFuncSum.setPushDownName(
                    pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("SUM")));
            Item sumFuncCount = new ItemField(null, null, colNameCount);
            sumFuncCount.setPushDownName(
                    pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("COUNT")));
            Item itemSum = createFieldItem(sumFuncSum, fields, startIndex);
            Item itemCount = createFieldItem(sumFuncCount, fields, startIndex);
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
            Item sumFuncCount = new ItemField(null, null, colNameCount);
            sumFuncCount.setPushDownName(
                    pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("COUNT")));
            Item sumFuncSum = new ItemField(null, null, colNameSum);
            sumFuncSum.setPushDownName(
                    pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("SUM")));
            Item sumFuncVar = new ItemField(null, null, colNameVar);
            sumFuncVar.setPushDownName(
                    pdName.replace(MysqlVisitor.getMadeAggAlias(funName), MysqlVisitor.getMadeAggAlias("VARIANCE")));
            Item itemCount = createFieldItem(sumFuncCount, fields, startIndex);
            Item itemSum = createFieldItem(sumFuncSum, fields, startIndex);
            Item itemVar = createFieldItem(sumFuncVar, fields, startIndex);
            args.add(itemCount);
            args.add(itemSum);
            args.add(itemVar);
        } else {
            Item subItem = createFieldItem(sumFunction, fields, startIndex);
            args.add(subItem);
        }
        ret = sumFunction.reStruct(args, true, fields);
        ret.setItemName(sumFunction.getPushDownName() == null ? sumFunction.getItemName() : sumFunction.getPushDownName());
        return ret;
    }

    private static ItemFunc createFunctionItem(ItemFunc f, List<Field> fields, int startIndex, boolean allPushDown,
                                               HandlerType type) {
        List<Item> args = new ArrayList<>();
        for (int index = 0; index < f.getArgCount(); index++) {
            Item arg = f.arguments().get(index);
            Item newArg;
            if (arg.isWild())
                newArg = new ItemInt(0);
            else
                newArg = createItem(arg, fields, startIndex, allPushDown, type);
            if (newArg == null)
                throw new RuntimeException("Function argument not found:" + arg);
            args.add(newArg);
        }
        ItemFunc ret = (ItemFunc) f.reStruct(args, allPushDown, fields);
        ret.setItemName(f.getPushDownName() == null ? f.getItemName() : f.getPushDownName());
        return ret;
    }

    /**
     * @param f           ItemSum
     * @param fields      List<Field>
     * @param startIndex  startIndex
     * @param allPushDown allPushDown
     * @param type        HandlerType
     * @return ItemSum
     */
    private static ItemSum createSumItem(ItemSum f, List<Field> fields, int startIndex, boolean allPushDown,
                                         HandlerType type) {
        List<Item> args = new ArrayList<>();
        for (int index = 0; index < f.getArgCount(); index++) {
            Item arg = f.arguments().get(index);
            Item newArg;
            if (arg.isWild())
                newArg = new ItemInt(0);
            else
                newArg = createItem(arg, fields, startIndex, allPushDown, type);
            if (newArg == null)
                throw new RuntimeException("Function argument not found:" + arg);
            args.add(newArg);
        }
        ItemSum ret = (ItemSum) f.reStruct(args, allPushDown, fields);
        ret.setItemName(f.getPushDownName() == null ? f.getItemName() : f.getPushDownName());
        if (ret instanceof ItemFuncGroupConcat && ((ItemFuncGroupConcat) ret).getOrders() != null) {
            for (Order order : ((ItemFuncGroupConcat) ret).getOrders()) {
                Item arg = order.getItem();
                Item newArg = createItem(arg, fields, startIndex, allPushDown, type);
                if (newArg == null)
                    throw new RuntimeException("Function argument not found:" + arg);
                order.setItem(newArg);
            }
        }
        return ret;
    }

    static ItemField createFieldItem(Item col, List<Field> fields, int startIndex) {
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
     * @param sel        sel
     * @param fields     fields
     * @param startIndex startIndex
     * @return index
     */
    public static int findField(Item sel, List<Field> fields, int startIndex) {
        String selName = (sel.getPushDownName() == null ? sel.getItemName() : sel.getPushDownName());
        selName = selName.trim();
        String tableName = sel.getTableName();
        String schemaName = sel.getDbName();
        for (int index = startIndex; index < fields.size(); index++) {
            Field field = fields.get(index);
            // field.name==null if '' push down
            String colName2 = field.getName() == null ? null : field.getName().trim();
            String tableName2 = field.getTable();
            String schemaName2 = field.getDbName();
            if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
                if (tableName2 != null) {
                    tableName2 = tableName2.toLowerCase();
                }
                if (schemaName2 != null) {
                    schemaName2 = schemaName2.toLowerCase();
                }
            }
            if (sel instanceof ItemField && !(StringUtil.equalsWithEmpty(tableName, tableName2) && isMatchSchema(schemaName, field.getOrgTable(), schemaName2))) {
                continue;
            }
            if (selName.equalsIgnoreCase(colName2)) {
                return index;
            }
        }
        return -1;
    }

    private static boolean isMatchSchema(String logicSchema, String table, String sourceSchema) {
        if (StringUtil.isEmpty(table) || StringUtil.isEmpty(logicSchema) || StringUtil.equalsWithEmpty(logicSchema, sourceSchema)) {
            return true;
        }
        SchemaConfig schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(logicSchema);
        if (schemaConfig.isNoSharding()) {
            PhysicalDataNode dbNode = DbleServer.getInstance().getConfig().getDataNodes().get(schemaConfig.getDataNode());
            return dbNode.getDatabase().equals(sourceSchema);
        }
        TableConfig tbConfig = schemaConfig.getTables().get(table);
        if (tbConfig == null) {
            PhysicalDataNode dbNode = DbleServer.getInstance().getConfig().getDataNodes().get(schemaConfig.getDataNode());
            return dbNode.getDatabase().equals(sourceSchema);
        } else {
            for (String dataNode : tbConfig.getDataNodes()) {
                PhysicalDataNode dbNode = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
                if (dbNode.getDatabase().equals(sourceSchema)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * make order by from distinct
     *
     * @param selects selects
     * @return order list
     */
    public static List<Order> makeOrder(List<Item> selects) {
        List<Order> orders = new ArrayList<>();
        for (Item sel : selects) {
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
