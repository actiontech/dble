/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.util;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.NamedField;
import com.actiontech.dble.plan.Order;
import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.PlanNode.PlanNodeType;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.Item.ItemType;
import com.actiontech.dble.plan.common.item.ItemBasicConstant;
import com.actiontech.dble.plan.common.item.ItemField;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.actiontech.dble.plan.common.item.function.ItemFunc.Functype;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum;
import com.actiontech.dble.plan.common.item.function.sumfunc.ItemSum.Sumfunctype;
import com.actiontech.dble.plan.node.JoinNode;
import com.actiontech.dble.plan.node.MergeNode;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.route.parser.util.Pair;

import java.util.*;

public final class PlanUtil {
    private PlanUtil() {
    }

    public static boolean existAggr(PlanNode node) {
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
     * @param column
     * @param node
     * @return the target table node and the column
     */
    public static Pair<TableNode, ItemField> findColumnInTableLeaf(ItemField column, PlanNode node) {
        // union return
        if (node.type() == PlanNodeType.MERGE)
            return null;
        NamedField tmpField = new NamedField(column.getTableName(), column.getItemName(), null);
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
     * @param f
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
        return;
    }

    /**
     * the sel can be pushed down to child?
     *
     * @param sel
     * @param child
     * @return
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
        if (sumFunc.sumType() == Sumfunctype.GROUP_CONCAT_FUNC)
            return true;
        if (sumFunc.hasWithDistinct())
            return true;
        return sumFunc.sumType() == Sumfunctype.UDF_SUM_FUNC;
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
        if (sel.getReferTables().size() > 1) {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "can not pushdown sel when refer table's > 1!");
        }
        if (sel instanceof ItemField) {
            return pushDownCol(node, (ItemField) sel);
        } else if (sel instanceof ItemFunc || sel instanceof ItemSum) {
            Item func = sel.cloneStruct();
            if (sel.getReferTables().isEmpty()) {
                func.setPushDownName(null);
                sel.setPushDownName(func.getItemName());
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
            sel.setPushDownName(func.getItemName());
            return func;
        } else {
            throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "", "not supported!");
        }
    }

    /**
     * is bf can be the joinkey for node?make bf's left related to left to node
     *
     * @param bf
     * @param node
     * @return
     */
    public static boolean isJoinKey(ItemFuncEqual bf, JoinNode node) {
        // only funEqula may become joinkey
        boolean isJoinKey = false;
        Item selCol = bf.arguments().get(0);
        Item selVal = bf.arguments().get(1);
        Set<PlanNode> colTns = selCol.getReferTables();
        Set<PlanNode> valTns = selVal.getReferTables();
        if (colTns.size() == 1 && valTns.size() == 1) {
            // a.id=b.id is join key,else not
            PlanNode colTn = colTns.iterator().next();
            PlanNode valTn = valTns.iterator().next();
            if (colTn == node.getLeftNode() && valTn == node.getRightNode()) {
                isJoinKey = true;
            } else if (colTn == node.getRightNode() && valTn == node.getLeftNode()) {
                isJoinKey = true;
                bf.arguments().set(0, selVal);
                bf.arguments().set(1, selCol);
            }
        }

        return isJoinKey;
    }

    /**
     * Join change the a.id=b.id in where filter in join node into join condition, and remove from where
     */
    public static void findJoinKeysAndRemoveIt(List<Item> dnfNode, JoinNode join) {
        if (dnfNode.isEmpty()) {
            return;
        }
        List<Item> joinFilters = new LinkedList<>();
        for (Item subItem : dnfNode) { // sinple condition
            if (subItem.type().equals(ItemType.FUNC_ITEM) || subItem.type().equals(ItemType.COND_ITEM)) {
                ItemFunc sub = (ItemFunc) subItem;
                if (!(sub.functype().equals(Functype.EQ_FUNC)))
                    continue;
                if (join.isInnerJoin() && isJoinKey((ItemFuncEqual) sub, join)) {
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
        if (node.getNoshardNode() != null & node.type() != PlanNodeType.TABLE) {
            return true;
        } else {
            return isERNode(node);
        }
    }

    public static boolean isGlobal(PlanNode node) {
        return node.getNoshardNode() != null && node.getUnGlobalTableCount() == 0;
    }

    /**
     * push function in merge node to child node
     *
     * @param mn
     * @param toPush
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
     * @param toPush
     * @param colIndexs
     * @param childSelects
     * @return
     */
    private static ItemFunc pushFunctionToUnionChild(ItemFunc toPush, Map<String, Integer> colIndexs,
                                                     List<Item> childSelects) {
        ItemFunc func = (ItemFunc) toPush.cloneStruct();
        for (int index = 0; index < toPush.getArgCount(); index++) {
            Item arg = toPush.arguments().get(index);
            Item pushedArg = pushItemToUnionChild(arg, colIndexs, childSelects);
            func.arguments().set(index, pushedArg);
        }
        func.setPushDownName(null);
        refreshReferTables(func);
        return func;
    }

    // ----------------help method------------

    private static Item pushDownCol(PlanNode node, ItemField col) {
        NamedField tmpField = new NamedField(col.getTableName(), col.getItemName(), null);
        NamedField coutField = node.getInnerFields().get(tmpField);
        return coutField.planNode.getOuterFields().get(coutField);
    }

    /**
     * generate push orders from orders node
     *
     * @param node
     * @param orders
     * @return
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
     * @param orders1
     * @param orders2
     * @return
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
}
