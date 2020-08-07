/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.util;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.privileges.ShardingPrivileges;
import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.*;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.ItemFunc.Functype;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.*;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondAnd;
import com.actiontech.dble.plan.common.item.function.operator.logic.ItemCondOr;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum.SumFuncType;
import com.actiontech.dble.plan.common.item.subquery.ItemAllAnySubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemExistsSubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemInSubQuery;
import com.actiontech.dble.plan.common.item.subquery.ItemScalarSubQuery;
import com.actiontech.dble.plan.common.ptr.BoolPtr;
import com.actiontech.dble.plan.node.*;
import com.actiontech.dble.plan.node.PlanNode.PlanNodeType;
import com.actiontech.dble.route.parser.util.Pair;

import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.util.*;

public final class PlanUtil {
    private PlanUtil() {
    }

    public static boolean existAggregate(PlanNode node) {
        if (node.getReferedTableNodes().size() == 0)
            return false;
        else {
            if (node.getSumFuncs().size() > 0 || node.getGroupBys().size() > 0 ||
                    node.getLimitTo() != -1 || node.isDistinct())
                return true;
        }
        return false;
    }

    /**
     * check obj is the column of an real tablenode ,if obj is not Column Type return null
     * <the column to be found must be  table's parent's column>
     *
     * @param column column
     * @param node   node
     * @return the target table node and the column
     */
    public static Pair<TableNode, ItemField> findColumnInTableLeaf(ItemField column, PlanNode node) {
        // union return
        if (node.type() == PlanNodeType.MERGE)
            return null;
        NamedField tmpField = new NamedField(column.getDbName(), column.getTableName(), column.getItemName(), null);
        NamedField coutField = node.getInnerFields().get(tmpField);
        if (coutField == null)
            return null;
        else if (node.type() == PlanNodeType.TABLE) {
            return new Pair<>((TableNode) node, column);
        } else {
            PlanNode referNode = column.getReferTables().iterator().next();
            Item cSel = referNode.getOuterFields().get(coutField);
            if (cSel instanceof ItemField) {
                return findColumnInTableLeaf((ItemField) cSel, referNode);
            } else {
                return null;
            }
        }
    }

    /**
     * refertable of refreshed function
     *
     * @param f is item
     */
    public static void refreshReferTables(Item f) {
        if (!(f instanceof ItemFunc || f instanceof ItemSum))
            return;
        HashSet<PlanNode> rtables = f.getReferTables();
        rtables.clear();
        for (int index = 0; index < f.getArgCount(); index++) {
            Item arg = f.arguments().get(index);
            rtables.addAll(arg.getReferTables());
            // refresh exist sel is null
            f.setWithIsNull(f.isWithIsNull() || arg.isWithIsNull());
            // refresh exist agg
            f.setWithSumFunc(f.isWithSumFunc() || arg.isWithSumFunc());
            f.setWithSubQuery(f.isWithSubQuery() || arg.isWithSubQuery());
            f.setWithUnValAble(f.isWithUnValAble() || arg.isWithUnValAble());
        }
    }

    /**
     * the sel can be pushed down to child?
     *
     * @param sel    item
     * @param child  child
     * @param parent parent
     * @return can push down
     */
    public static boolean canPush(Item sel, PlanNode child, PlanNode parent) {
        if (sel == null)
            return false;
        if (sel.isWithSumFunc())
            return false;
        HashSet<PlanNode> referTables = sel.getReferTables();
        if (referTables.size() == 0) {
            return true;
        } else if (referTables.size() == 1) {
            PlanNode referTable = referTables.iterator().next();
            if (referTable == child) {
                // if left join's right node's is null condition will not be pushed down
                return !sel.isWithIsNull() || parent.type() != PlanNodeType.JOIN || !((JoinNode) parent).isLeftOuterJoin() ||
                        ((JoinNode) parent).getRightNode() != child;
            }
        }
        return false;
    }

    /**
     * isDirectPushDownFunction
     **/
    public static boolean isDirectPushDownFunction(Item func) {
        if (func.isWithSumFunc())
            return false;
        else {
            // push down if func's related table is only 1
            return func.getReferTables().size() <= 1;
        }
    }

    public static boolean isUnPushDownSum(ItemSum sumFunc) {
        if (sumFunc.sumType() == SumFuncType.GROUP_CONCAT_FUNC)
            return true;
        if (sumFunc.hasWithDistinct())
            return true;
        return sumFunc.sumType() == SumFuncType.UDF_SUM_FUNC;
    }

    public static Item pushDownItem(PlanNode node, Item sel) {
        return pushDownItem(node, sel, false);
    }

    // push down node's sel
    public static Item pushDownItem(PlanNode node, Item sel, boolean subQueryOpt) {
        if (sel == null)
            return null;
        if (sel.basicConstItem())
            return sel;
        if (sel.isWithSubQuery()) {
            return sel;
        }
        if (sel.getReferTables().size() > 1) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can not pushdown sel when refer table's > 1!");
        }
        if (sel instanceof ItemField) {
            return pushDownCol(node, (ItemField) sel);
        } else if (sel instanceof ItemFunc || sel instanceof ItemSum) {
            Item func = sel.cloneStruct();
            if (sel.getReferTables().isEmpty()) {
                func.setPushDownName(null);
                sel.setPushDownName(sel.getAlias() != null ? sel.getAlias() : func.getItemName());
                return func;
            }
            if (!subQueryOpt && (func.isWithSumFunc())) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can not pushdown sum func!");
            }
            for (int index = 0; index < func.getArgCount(); index++) {
                Item arg = func.arguments().get(index);
                Item newArg = pushDownItem(node, arg, subQueryOpt);
                func.arguments().set(index, newArg);
            }
            refreshReferTables(func);
            func.setPushDownName(null);
            sel.setPushDownName(sel.getAlias() != null ? sel.getAlias() : func.getItemName());
            return func;
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported!");
        }
    }

    /**
     * is bf can be the joinkey for node?make bf's left related to left to node
     *
     * @param bf   ItemFuncEqual
     * @param node node
     * @return is Join Key
     */
    public static boolean isJoinColumn(ItemFuncEqual bf, JoinNode node) {
        // only funEqula may become joinkey
        boolean isJoinColumn = false;
        Item selCol = bf.arguments().get(0);
        Item selVal = bf.arguments().get(1);
        Set<PlanNode> colTns = selCol.getReferTables();
        Set<PlanNode> valTns = selVal.getReferTables();
        if (colTns.size() == 1 && valTns.size() == 1) {
            // a.id=b.id is join key,else not
            PlanNode colTn = colTns.iterator().next();
            PlanNode valTn = valTns.iterator().next();
            if (colTn == node.getLeftNode() && valTn == node.getRightNode()) {
                isJoinColumn = true;
            } else if (colTn == node.getRightNode() && valTn == node.getLeftNode()) {
                isJoinColumn = true;
                bf.arguments().set(0, selVal);
                bf.arguments().set(1, selCol);
            }
        }

        return isJoinColumn;
    }

    /**
     * Join change the a.id=b.id in where filter in join node into join condition, and remove from where
     */
    public static void findJoinColumnsAndRemoveIt(List<Item> dnfNode, JoinNode join) {
        if (dnfNode.isEmpty()) {
            return;
        }
        List<Item> joinFilters = new LinkedList<>();
        for (Item subItem : dnfNode) { // sinple condition
            if (subItem.type().equals(ItemType.FUNC_ITEM) || subItem.type().equals(ItemType.COND_ITEM)) {
                ItemFunc sub = (ItemFunc) subItem;
                if (!(sub.functype().equals(Functype.EQ_FUNC)))
                    continue;
                if (join.isInnerJoin() && isJoinColumn((ItemFuncEqual) sub, join)) {
                    joinFilters.add(sub);
                    join.addJoinFilter((ItemFuncEqual) sub);
                }
            }
        }
        dnfNode.removeAll(joinFilters);
    }

    public static boolean isERNode(PlanNode node) {
        if (node instanceof JoinNode) {
            JoinNode join = (JoinNode) node;
            if (join.getERkeys() != null && join.getERkeys().size() > 0) {
                return true;
            }
        }
        return false;
    }

    public static boolean isGlobalOrER(PlanNode node) {
        return !node.isContainsSubQuery() && ((node.getNoshardNode() != null && node.type() != PlanNodeType.TABLE && node.type() != PlanNodeType.QUERY) || isERNode(node)) && (hasNoFakeNode(node) || (node instanceof NoNameNode));
    }

    public static boolean isGlobal(PlanNode node) {
        return node.getNoshardNode() != null && node.getUnGlobalTableCount() == 0 && !node.isContainsSubQuery() && (hasNoFakeNode(node) || (node instanceof NoNameNode));
    }


    public static boolean hasNoFakeNode(PlanNode node) {
        if (node instanceof NoNameNode) {
            return !((NoNameNode) node).isFakeNode();
        }
        if (node.getChildren() != null) {
            for (PlanNode n : node.getChildren()) {
                if (!hasNoFakeNode(n)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * push function in merge node to child node
     *
     * @param mn        MergeNode
     * @param toPush    Item toPush
     * @param colIndexs colIndex
     * @return all child's pushdown sel
     */
    public static List<Item> getPushItemsToUnionChild(MergeNode mn, Item toPush, Map<String, Integer> colIndexs) {
        if (toPush == null)
            return null;
        List<Item> pusheds = new ArrayList<>();
        for (int i = 0; i < mn.getChildren().size(); i++) {
            Item pushed = pushItemToUnionChild(toPush, colIndexs, mn.getChildren().get(i).getColumnsSelected());
            pusheds.add(pushed);
        }
        return pusheds;
    }

    private static Item pushItemToUnionChild(Item toPush, Map<String, Integer> colIndexs, List<Item> childSelects) {
        if (toPush instanceof ItemField) {
            return pushColToUnionChild((ItemField) toPush, colIndexs, childSelects);
        } else if (toPush instanceof ItemFunc) {
            return pushFunctionToUnionChild((ItemFunc) toPush, colIndexs, childSelects);
        } else if (toPush instanceof ItemBasicConstant) {
            return toPush;
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "unexpected!");
        }
    }

    private static Item pushColToUnionChild(ItemField toPush, Map<String, Integer> colIndexs, List<Item> childSelects) {
        String name = toPush.getItemName();
        int colIndex = colIndexs.get(name);
        return childSelects.get(colIndex);
    }

    /**
     * arg of merge node's function belong to child
     *
     * @param toPush       toPush
     * @param colIndexs    colIndexs
     * @param childSelects childSelects
     * @return ItemFunc
     */
    private static ItemFunc pushFunctionToUnionChild(ItemFunc toPush, Map<String, Integer> colIndexs, List<Item> childSelects) {
        ItemFunc func = (ItemFunc) toPush.cloneStruct();
        for (int index = 0; index < toPush.getArgCount(); index++) {
            Item arg = toPush.arguments().get(index);
            Item pushedArg = pushItemToUnionChild(arg, colIndexs, childSelects);
            func.arguments().set(index, pushedArg);
        }
        refreshReferTables(func);
        return func;
    }

    // ----------------help method------------

    private static Item pushDownCol(PlanNode node, ItemField col) {
        NamedField tmpField = new NamedField(col.getDbName(), col.getTableName(), col.getItemName(), null);
        NamedField coutField = node.getInnerFields().get(tmpField);
        return coutField.planNode.getOuterFields().get(coutField);
    }

    /**
     * generate push orders from orders node
     *
     * @param node   node
     * @param orders orders
     * @return List<Order>
     */
    public static List<Order> getPushDownOrders(PlanNode node, List<Order> orders) {
        List<Order> pushOrders = new ArrayList<>();
        for (Order order : orders) {
            Item newSel = pushDownItem(node, order.getItem());
            Order newOrder = new Order(newSel, order.getSortOrder());
            pushOrders.add(newOrder);
        }
        return pushOrders;
    }

    /**
     * when order1contains order2,return true, order2's column start from order1's first column
     *
     * @param orders1 orders1
     * @param orders2 orders2
     * @return orderContains
     */
    public static boolean orderContains(List<Order> orders1, List<Order> orders2) {
        if (orders1.size() < orders2.size())
            return false;
        else {
            for (int index = 0; index < orders2.size(); index++) {
                Order order2 = orders2.get(index);
                Order order1 = orders1.get(index);
                if (!order2.equals(order1)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static boolean isCmpFunc(Item filter) {
        return filter instanceof ItemFuncEqual || filter instanceof ItemFuncGt || filter instanceof ItemFuncGe ||
                filter instanceof ItemFuncLt || filter instanceof ItemFuncLe || filter instanceof ItemFuncNe ||
                filter instanceof ItemFuncStrictEqual || filter instanceof ItemFuncLike;
    }

    public static boolean containsSubQuery(PlanNode node) {
        if (node.isWithSubQuery()) {
            return true;
        }
        for (PlanNode child : node.getChildren()) {
            if (containsSubQuery(child)) {
                return true;
            }
        }
        return false;
    }


    public static Item rebuildSubQueryItem(Item item) {
        if (!item.isWithSubQuery()) {
            return item;
        }
        BoolPtr reBuild = new BoolPtr(false);
        if (PlanUtil.isCmpFunc(item)) {
            Item res1 = rebuildBoolSubQuery(item, 0, reBuild, new BoolPtr(false), new BoolPtr(false));
            if (res1 != null) {
                return res1;
            }

            BoolPtr needExecuteNull = new BoolPtr(false);
            BoolPtr isAll = new BoolPtr(false);
            Item res2 = rebuildBoolSubQuery(item, 1, reBuild, needExecuteNull, isAll);
            if (res2 != null) {
                return res2;
            }
            if (needExecuteNull.get()) {
                return getForAllAnyWithNullItem(item, isAll);
            }
        } else if (item instanceof ItemInSubQuery) {
            return genInsubQueryItem((ItemInSubQuery) item);
        } else if (item instanceof ItemExistsSubQuery) {
            ItemExistsSubQuery existsSubQuery = (ItemExistsSubQuery) item;
            Item result = existsSubQuery.getValue();
            if (result == null) {
                return genBoolItem(existsSubQuery.isNot());
            } else {
                return genBoolItem(!existsSubQuery.isNot());
            }
        } else if (item instanceof ItemCondAnd || item instanceof ItemCondOr) {
            for (int index = 0; index < item.getArgCount(); index++) {
                Item rebuildItem = rebuildSubQueryItem(item.arguments().get(index));
                item.arguments().set(index, rebuildItem);
                item.setItemName(null);
            }
        } else if (item instanceof ItemScalarSubQuery) {
            Item result = ((ItemScalarSubQuery) item).getValue();
            if (result == null || result.getResultItem() == null) {
                return new ItemNull();
            }
            return result.getResultItem();
        }
        if (!reBuild.get() && item instanceof ItemFunc) {
            return rebuildSubQueryFuncItem(item);
        }
        item.setWithSubQuery(false);
        return item;
    }

    public static Item genInsubQueryItem(ItemInSubQuery item) {
        ItemInSubQuery inSubItem = item;
        if (inSubItem.getValue().size() == 0 && !inSubItem.isContainNull()) {
            return genBoolItem(inSubItem.isNeg());
        } else if (inSubItem.getValue().size() == 0 && inSubItem.isContainNull()) {
            return new ItemNull();
        } else {
            int argSize = inSubItem.getValue().size() + 1;
            if (inSubItem.isContainNull()) {
                argSize++;
            }
            List<Item> args = new ArrayList<>(argSize);
            args.add(inSubItem.getLeftOperand());
            if (inSubItem.isContainNull()) {
                args.add(new ItemNull());
            }
            args.addAll(inSubItem.getValue());
            return new ItemFuncIn(args, inSubItem.isNeg(), inSubItem.getCharsetIndex());
        }
    }

    public static Item getForAllAnyWithNullItem(Item item, BoolPtr isAll) {
        List<Item> args = new ArrayList<>(2);
        args.add(item);
        args.add(new ItemNull());
        if (isAll.get()) {
            return new ItemCondAnd(args);
        } else {
            return new ItemCondOr(args);
        }
    }

    private static Item rebuildSubQueryFuncItem(Item item) {
        ItemFunc func = (ItemFunc) item;
        item.setWithSubQuery(false);
        Item itemTmp = item.cloneItem();
        for (int index = 0; index < func.getArgCount(); index++) {
            Item arg = item.arguments().get(index);
            if (arg instanceof ItemScalarSubQuery) {
                Item result = ((ItemScalarSubQuery) arg).getValue();
                if (result == null || result.getResultItem() == null) {
                    itemTmp.arguments().set(index, new ItemNull());
                } else {
                    itemTmp.arguments().set(index, result.getResultItem());
                }
            } else if (arg instanceof ItemInSubQuery) {
                ItemInSubQuery inSubItem = (ItemInSubQuery) arg;
                if (inSubItem.getValue().size() == 0 && !inSubItem.isContainNull()) {
                    itemTmp.arguments().set(index, genBoolItem(inSubItem.isNeg()));
                } else if (inSubItem.getValue().size() == 0 && inSubItem.isContainNull()) {
                    itemTmp.arguments().set(index, new ItemNull());
                } else {
                    int argSize = inSubItem.getValue().size() + 1;
                    if (inSubItem.isContainNull()) {
                        argSize++;
                    }
                    List<Item> newArgs = new ArrayList<>(argSize);
                    newArgs.add(inSubItem.getLeftOperand());
                    if (inSubItem.isContainNull()) {
                        newArgs.add(new ItemNull());
                    }
                    newArgs.addAll(inSubItem.getValue());
                    itemTmp.arguments().set(index, new ItemFuncIn(newArgs, inSubItem.isNeg(), inSubItem.getCharsetIndex()));
                }
            } else if (arg instanceof ItemFunc) {
                itemTmp.arguments().set(index, rebuildSubQueryItem(arg));
            }
        }
        itemTmp.setItemName(null);
        return itemTmp;
    }


    private static Item genBoolItem(boolean isTrue) {
        if (isTrue) {
            return new ItemFuncEqual(new ItemInt(1), new ItemInt(1), 63);
        } else {
            return new ItemFuncEqual(new ItemInt(1), new ItemInt(0), 63);
        }
    }

    private static Item rebuildBoolSubQuery(Item item, int index, BoolPtr reBuild, BoolPtr needExecuteNull, BoolPtr isAll) {
        Item arg = item.arguments().get(index);
        if (arg.type().equals(ItemType.SUBSELECT_ITEM)) {
            if (arg instanceof ItemScalarSubQuery) {
                Item result = ((ItemScalarSubQuery) arg).getValue();
                if (result == null || result.getResultItem() == null) {
                    reBuild.set(true);
                    return new ItemNull();
                }
                item.arguments().set(index, result.getResultItem());
                item.setItemName(null);
            } else if (arg instanceof ItemAllAnySubQuery) {
                ItemAllAnySubQuery allAnySubItem = (ItemAllAnySubQuery) arg;
                if (allAnySubItem.getValue().size() == 0 && !allAnySubItem.isContainNull()) {
                    reBuild.set(true);
                    return genBoolItem(allAnySubItem.isAll());
                } else if (allAnySubItem.getValue().size() == 0 && allAnySubItem.isContainNull()) {
                    reBuild.set(true);
                    return new ItemNull();
                } else if (allAnySubItem.getValue().size() == 1 && !allAnySubItem.isContainNull()) {
                    Item value = allAnySubItem.getValue().get(0);
                    item.arguments().set(index, value.getResultItem());
                    item.setItemName(null);
                } else if (allAnySubItem.getValue().size() == 1 && allAnySubItem.isContainNull()) {
                    // item operator all (some_value,null)
                    //  =>item operator some_value and item operator null
                    //  =>item operator some_value and unknown

                    // item operator any (some_value,null)
                    //  =>item operator some_value or item operator null
                    //  =>item operator some_value or unknown
                    needExecuteNull.set(true);
                    isAll.set(allAnySubItem.isAll());
                    Item value = allAnySubItem.getValue().get(0);
                    item.arguments().set(index, value.getResultItem());
                } else {
                    reBuild.set(true);
                    /* item = all (value1,value2,null)
                      =>item = value1 and item = value2 and unknown
                      =>false and unknown  =>false*/

                    /* item <> any (value1,value2,null)
                      =>item <> value1 or item <> value2 or unknown
                      =>true or unknown  =>true*/
                    return genBoolItem(!allAnySubItem.isAll());

                }
            }
        }
        return null;
    }


    public static void checkTablesPrivilege(ShardingService service, PlanNode node, SQLSelectStatement stmt) {
        for (TableNode tn : node.getReferedTableNodes()) {
            if (!ShardingPrivileges.checkPrivilege(service.getUserConfig(), tn.getSchema(), tn.getTableName(), ShardingPrivileges.CheckType.SELECT)) {
                String msg = "The statement DML privilege check is not passed, sql:" + stmt.toString().replaceAll("[\\t\\n\\r]", " ");
                throw new MySQLOutPutException(ErrorCode.ER_PARSE_ERROR, "", msg);
            }
        }
    }

}
