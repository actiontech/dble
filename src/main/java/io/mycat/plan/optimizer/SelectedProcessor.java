package io.mycat.plan.optimizer;

import io.mycat.plan.Order;
import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.ItemInt;
import io.mycat.plan.common.item.function.ItemFunc;
import io.mycat.plan.common.item.function.sumfunc.ItemSum;
import io.mycat.plan.node.MergeNode;
import io.mycat.plan.util.PlanUtil;

import java.util.*;

/**
 * 尽量使得select项在传输过程中最少
 *
 * @author ActionTech
 */
public final class SelectedProcessor {
    private SelectedProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        qtn = pushSelected(qtn, new HashSet<Item>());
        return qtn;
    }

    /**
     * 将topushColumns列下推到当前节点上 父节点的selectedrefered为A.id,A.name,B.id,B.name,
     * name子节点A只要提供父节点的selectcolumn为 A.id,A.name即可
     *
     * @param qtn
     * @param toPushColumns
     * @return
     */
    private static PlanNode pushSelected(PlanNode qtn, Collection<Item> toPushColumns) {
        boolean isPushDownNode = false;
        if (PlanUtil.isGlobalOrER(qtn)) {
            // 这边应该循环遍历它的child然后每个进行buildColumnRefers,先不处理了
            List<Item> selList = qtn.getColumnsSelected();
            for (Item pdSel : toPushColumns) {
                if (!selList.contains(pdSel)) {
                    selList.add(pdSel);
                }
            }
            isPushDownNode = true;
            qtn.setUpRefers(isPushDownNode);
            return qtn;
        }
        isPushDownNode = (qtn.type() == PlanNodeType.TABLE || qtn.type() == PlanNodeType.NONAME);
        if (qtn.type() == PlanNodeType.MERGE) {
            return mergePushSelected((MergeNode) qtn, toPushColumns);
        } else {
            if (toPushColumns.isEmpty()) {
                qtn.setUpRefers(isPushDownNode);
            } else if (qtn.isDistinct()) {
                List<Item> selList = qtn.getColumnsSelected();
                for (Item pdSel : toPushColumns) {
                    if (!selList.contains(pdSel)) {
                        selList.add(pdSel);
                    }
                }
                qtn.setUpRefers(isPushDownNode);
            } else {
                List<Item> selList = qtn.getColumnsSelected();
                selList.clear();
                boolean existSum = false;
                for (Item toPush : toPushColumns) {
                    selList.add(toPush);
                    existSum |= toPush.type().equals(ItemType.SUM_FUNC_ITEM);
                }
                // @bug select sum(id) from (select id,sum(id) from t1) t
                // 如果直接将id下推,少去了sum(id)则出错
                if (!existSum && qtn.getSumFuncs().size() > 0) {
                    selList.add(qtn.getSumFuncs().iterator().next());
                }
                qtn.setUpRefers(isPushDownNode);
            }
            PlanNodeType i = qtn.type();
            if (i == PlanNodeType.NONAME) {
                return qtn;
            } else if (i == PlanNodeType.TABLE) {
                return qtn;
            } else {
                for (PlanNode child : qtn.getChildren()) {
                    List<Item> referList = qtn.getColumnsReferedByChild(child);
                    if (referList.isEmpty()) {
                        referList.add(new ItemInt(1));
                    }
                    Collection<Item> pdRefers = getPushDownSel(qtn, child, referList);
                    pushSelected(child, pdRefers);
                }
                return qtn;
            }
        }
    }

    private static Collection<Item> getPushDownSel(PlanNode parent, PlanNode child, List<Item> selList) {
        // oldselectable->newselectbable
        HashMap<Item, Item> oldNewMap = new HashMap<>();
        HashMap<Item, Item> oldKeyKeyMap = new HashMap<>();
        for (Item sel : selList) {
            Item pdSel = oldNewMap.get(sel);
            if (pdSel == null) {
                pdSel = PlanUtil.pushDownItem(parent, sel);
                oldNewMap.put(sel, pdSel);
                oldKeyKeyMap.put(sel, sel);
            } else {
                Item sameKey = oldKeyKeyMap.get(sel);
                sel.setPushDownName(sameKey.getPushDownName());
            }
        }
        return oldNewMap.values();
    }

    // union的push须区别于普通的push,toPushColumn.isEmpty时需保持merge的select属性不能被修改
    private static PlanNode mergePushSelected(MergeNode merge, Collection<Item> toPushColumns) {
        if (toPushColumns.isEmpty() && merge.getOrderBys().isEmpty()) {
            for (PlanNode child : merge.getChildren()) {
                pushSelected(child, new HashSet<Item>());
            }
            return merge;
        }
        boolean canOverload = mergeNodeChildsCheck(merge) && !toPushColumns.isEmpty();
        final Map<String, Integer> colIndexs = merge.getColIndexs();
        List<Item> mergeSelects = null;
        if (toPushColumns.isEmpty()) {
            // 不修改merge的select属性
            mergeSelects = new ArrayList<>();
            merge.setComeInFields(mergeSelects);
            mergeSelects.addAll(merge.getColumnsSelected());
        } else {
            mergeSelects = merge.getColumnsSelected();
        }
        if (canOverload) {
            mergeSelects.clear();
            mergeSelects.addAll(toPushColumns);
        } else {
            for (Item toPush : toPushColumns) {
                if (!mergeSelects.contains(toPush)) {
                    mergeSelects.add(toPush);
                }
            }
        }
        // 把order by添加进来
        for (Order orderby : merge.getOrderBys()) {
            Item orderSel = orderby.getItem();
            mergePushOrderBy(orderSel, mergeSelects);
        }
        // 将mergeselect中的内容下推到child中去
        List<List<Item>> allChildPushs = new ArrayList<>(toPushColumns.size());
        for (Item toPush : mergeSelects) {
            // union的order by必须从selects中直接查找
            if (toPush.getPushDownName() == null && !toPush.type().equals(ItemType.FIELD_ITEM))
                toPush.setPushDownName(toPush.getItemName());
            List<Item> childPushs = PlanUtil.getPushItemsToUnionChild(merge, toPush, colIndexs);
            allChildPushs.add(childPushs);
        }
        // 保证每个child的下推个数都是一致的
        for (int index = 0; index < merge.getChildren().size(); index++) {
            List<Item> colSels = merge.getChildren().get(index).getColumnsSelected();
            colSels.clear();
            for (List<Item> childPushs : allChildPushs) {
                colSels.add(childPushs.get(index));
            }
            pushSelected(merge.getChildren().get(index), new HashSet<Item>());
        }
        return merge;
    }

    /**
     * 检查merge下的subchild是否存在distinct或者聚合函数
     *
     * @param merge
     * @return
     */
    private static boolean mergeNodeChildsCheck(MergeNode merge) {
        for (PlanNode child : merge.getChildren()) {
            boolean cdis = child.isDistinct();
            boolean bsum = child.getSumFuncs().size() > 0;
            if (cdis || bsum)
                return false;
        }
        return true;
    }

    private static void mergePushOrderBy(Item orderSel, List<Item> mergeSelects) {
        if (orderSel instanceof ItemField) {
            if (!mergeSelects.contains(orderSel))
                mergeSelects.add(orderSel);
        } else if (orderSel instanceof ItemFunc) {
            ItemFunc func = (ItemFunc) orderSel;
            if (func.isWithSumFunc()) {
                for (int index = 0; index < func.getArgCount(); index++) {
                    Item arg = func.arguments().get(index);
                    mergePushOrderBy(arg, mergeSelects);
                }
            } else {
                if (!mergeSelects.contains(func)) {
                    mergeSelects.add(func);
                }
                // union的order by必须从selects中直接查找
                func.setPushDownName(func.getItemName());
            }
        } else if (orderSel instanceof ItemSum) {
            ItemSum func = (ItemSum) orderSel;
            for (int index = 0; index < func.getArgCount(); index++) {
                Item arg = func.arguments().get(index);
                mergePushOrderBy(arg, mergeSelects);
            }
        }
    }

}
