package io.mycat.plan.util;

import io.mycat.config.ErrorCode;
import io.mycat.plan.NamedField;
import io.mycat.plan.Order;
import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.item.ItemBasicConstant;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.ItemFunc.Functype;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;
import io.mycat.plan.common.item.function.sumfunc.ItemSum.Sumfunctype;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.MergeNode;
import io.mycat.plan.node.TableNode;
import io.mycat.route.parser.util.Pair;

import java.util.*;

public class PlanUtil {
    public static boolean existAggr(PlanNode node) {
        if (node.getReferedTableNodes().size() == 0)
            return false;
        else {
            if (node.sumFuncs.size() > 0 || node.getGroupBys().size() > 0 ||
                    node.getLimitTo() != -1 || node.isDistinct())
                return true;
        }
        return false;
    }

    /**
     * 查找obj在node树当中真正属于的tablenode的column 如果obj不是Column类型返回null
     * <查找的column必须是table的上级的column>
     *
     * @param column
     * @param node
     * @return 找到的那个tablenode以及column属性
     */
    public static Pair<TableNode, ItemField> findColumnInTableLeaf(ItemField column, PlanNode node) {
        // union的不可下查
        if (node.type() == PlanNodeType.MERGE)
            return null;
        NamedField tmpField = new NamedField(column.getTableName(), column.getItemName(), null);
        NamedField coutField = node.getInnerFields().get(tmpField);
        if (coutField == null)
            return null;
        else if (node.type() == PlanNodeType.TABLE) {
            return new Pair<TableNode, ItemField>((TableNode) node, column);
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
     * 刷新function的refertable
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
            f.withIsNull = f.withIsNull || arg.withIsNull;
            // refresh exist agg
            f.withSumFunc = f.withSumFunc || arg.withSumFunc;
            f.withSubQuery = f.withSubQuery || arg.withSubQuery;
            f.withUnValAble = f.withUnValAble || arg.withUnValAble;
        }
        return;
    }

    /**
     * 判断sel是否可以下推到child中
     *
     * @param sel
     * @param child
     * @return
     */
    public static boolean canPush(Item sel, PlanNode child, PlanNode parent) {
        if (sel == null)
            return false;
        if (sel.withSumFunc)
            return false;
        HashSet<PlanNode> referTables = sel.getReferTables();
        if (referTables.size() == 0) {
            return true;
        } else if (referTables.size() == 1) {
            PlanNode referTable = referTables.iterator().next();
            if (referTable == child) {
                // left join的right 节点的is null不下发
                if (sel.withIsNull && parent.type() == PlanNodeType.JOIN && ((JoinNode) parent).isLeftOuterJoin() &&
                        ((JoinNode) parent).getRightNode() == child)
                    return false;
                else
                    return true;
            }
        }
        return false;
    }

    /**
     * 是否属于可直接下推的函数
     **/
    public static boolean isDirectPushDownFunction(Item func) {
        if (func.withSumFunc)
            return false;
        else {
            // 如果函数涉及的所有的参数仅有一个table，则可以下推
            if (func.getReferTables().size() <= 1)
                return true;
            else
                return false;
        }
    }

    public static boolean isUnPushDownSum(ItemSum sumFunc) {
        if (sumFunc.sumType() == Sumfunctype.GROUP_CONCAT_FUNC)
            return true;
        if (sumFunc.hasWithDistinct())
            return true;
        if (sumFunc.sumType() == Sumfunctype.UDF_SUM_FUNC)
            return true;
        return false;
    }

    public static Item pushDownItem(PlanNode node, Item sel) {
        return pushDownItem(node, sel, false);
    }

    // 将node中的sel进行下推
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
            if (!subQueryOpt && (func.withSumFunc)) {
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
     * 判断bf是否可以做为node的joinkey，并且调整bf的左右位置
     *
     * @param bf
     * @param node
     * @return
     */
    public static boolean isJoinKey(ItemFuncEqual bf, JoinNode node) {
        // 只有等于的才可能是joinkey
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
     * 将原本的Join的where条件中的a.id=b.id构建为join条件，并从where条件中移除
     */
    public static void findJoinKeysAndRemoveIt(List<Item> dnfNode, JoinNode join) {
        if (dnfNode.isEmpty()) {
            return;
        }
        List<Item> joinFilters = new LinkedList<Item>();
        for (Item subItem : dnfNode) { // 一定是简单条件
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
        if (node.getNoshardNode() != null)
            return true;
        else {
            return isERNode(node);
        }
    }

    /**
     * 将merge节点上的函数下推到下面的child节点上 目前仅下推filter以及自定义函数
     *
     * @param mn
     * @param toPush
     * @return 返回所有child对应的pushdown sel
     */
    public static List<Item> getPushItemsToUnionChild(MergeNode mn, Item toPush, Map<String, Integer> colIndexs) {
        if (toPush == null)
            return null;
        List<Item> pusheds = new ArrayList<Item>();
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
     * merge的function的每一个arg一定属于child
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
     * 生成node中的orders的pushorders
     *
     * @param node
     * @param orders
     * @return
     */
    public static List<Order> getPushDownOrders(PlanNode node, List<Order> orders) {
        List<Order> pushOrders = new ArrayList<Order>();
        for (Order order : orders) {
            Item newSel = pushDownItem(node, order.getItem());
            Order newOrder = new Order(newSel, order.getSortOrder());
            pushOrders.add(newOrder);
        }
        return pushOrders;
    }

    /**
     * 当order1 包含order2的时候，返回true，要求order2的列在order1中要从第一列开始按照顺序来进行
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


//    public static boolean existPartiTable(PlanNode tn) {
//        boolean existNumParti = false;
//        for (TableNode table : tn.getReferedTableNodes()) {
//            if (table.isPartitioned()) {
//                existNumParti = true;
//                break;
//            }
//        }
//        return existNumParti;
//    }
}
