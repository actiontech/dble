package io.mycat.plan.optimizer;

import io.mycat.plan.Order;
import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.ptr.BoolPtr;
import io.mycat.plan.node.QueryNode;
import io.mycat.plan.node.view.ViewUtil;
import io.mycat.plan.util.FilterUtils;
import io.mycat.plan.util.PlanUtil;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 将View进行处理，将虚拟的merge node变成其它的三种类型的node
 *
 * @author ActionTech
 */
public final class SubQueryProcessor {
    private SubQueryProcessor() {
    }

    public static PlanNode optimize(PlanNode qtn) {
        BoolPtr merged = new BoolPtr(false);
        qtn = tryTransformerQuery(qtn, merged);
        if (merged.get())
            qtn.setUpFields();
        return qtn;
    }

    /**
     * 尝试找出qtn中的querynode，并将他们转换为对应的三种node
     *
     * @param qtn
     * @return
     */
    private static PlanNode tryTransformerQuery(PlanNode qtn, BoolPtr boolptr) {
        boolean childMerged = false;
        for (int index = 0; index < qtn.getChildren().size(); index++) {
            PlanNode child = qtn.getChildren().get(index);
            BoolPtr cbptr = new BoolPtr(false);
            PlanNode newChild = tryTransformerQuery(child, cbptr);
            if (cbptr.get())
                childMerged = true;
            qtn.getChildren().set(index, newChild);
        }
        if (childMerged)
            qtn.setUpFields();
        if (qtn.type() == PlanNodeType.QUERY) {
            qtn = transformerQuery((QueryNode) qtn, boolptr);
        }
        return qtn;
    }

    /**
     * 转换一个querynode
     *
     * @param query
     * @return
     */
    private static PlanNode transformerQuery(QueryNode query, BoolPtr boolptr) {
        boolean canBeMerged = ViewUtil.canBeMerged(query.getChild());
        if (canBeMerged) {
            // 需要将viewnode的属性merge到view的child，从而实现优化
            PlanNode newNode = mergeNode(query, query.getChild());
            boolptr.set(true);
            return newNode;
        } else {
            return query;
        }
    }

    /**
     * 将parent的属性merge到child，并返回新的child前提是child是canBeMerged
     *
     * @param parent
     * @param child
     * @return
     */
    private static PlanNode mergeNode(PlanNode parent, PlanNode child) {
        final List<Item> newSels = mergeSelect(parent, child);
        mergeWhere(parent, child);
        mergeGroupBy(parent, child);
        mergeHaving(parent, child);
        mergeOrderBy(parent, child);
        mergeLimit(parent, child);
        child.setColumnsSelected(newSels);
        if (!StringUtils.isEmpty(parent.getAlias()))
            child.setAlias(parent.getAlias());
        else if (!StringUtils.isEmpty(parent.getSubAlias()))
            child.setAlias(parent.getSubAlias());
        child.setSubQuery(parent.isSubQuery());
        child.setParent(parent.getParent());
        return child;
    }

    /**
     * view v_t1 as select id+1 idd from t1 tt1 order by idd select view
     * sql:select idd + 1 from v_t1 ==> select (id+1) + 1 from t1 tt1 order by
     * id+1
     *
     * @return 返回child应该保留的新的select信息
     * @notice 交给Mysqlvisitor执行
     */

    private static List<Item> mergeSelect(PlanNode parent, PlanNode child) {
        List<Item> pSels = parent.getColumnsSelected();
        List<Item> cNewSels = new ArrayList<Item>();
        for (Item pSel : pSels) {
            Item pSel0 = PlanUtil.pushDownItem(parent, pSel, true);
            String selName = pSel.getAlias();
            if (StringUtils.isEmpty(selName)) {
                selName = pSel.getItemName();
                // 下推时，父节点是函数，且函数无别名，mysql不允许select func() as func()这种
                if (pSel.type() == ItemType.FUNC_ITEM || pSel.type() == ItemType.COND_ITEM ||
                        pSel.type() == ItemType.SUM_FUNC_ITEM)
                    selName = Item.FNAF + selName;
            }
            pSel0.setAlias(selName);
            cNewSels.add(pSel0);
        }
        return cNewSels;
    }

    private static void mergeWhere(PlanNode parent, PlanNode child) {
        Item pWhere = parent.getWhereFilter();
        Item pWhere0 = PlanUtil.pushDownItem(parent, pWhere, true);
        Item mWhere = FilterUtils.and(pWhere0, child.getWhereFilter());
        child.setWhereFilter(mWhere);
    }

    private static void mergeGroupBy(PlanNode parent, PlanNode child) {
        List<Order> pGroups = parent.getGroupBys();
        List<Order> cGroups = new ArrayList<Order>();
        for (Order pGroup : pGroups) {
            Item col = pGroup.getItem();
            Item col0 = PlanUtil.pushDownItem(parent, col);
            Order pGroup0 = new Order(col0, pGroup.getSortOrder());
            cGroups.add(pGroup0);
        }
        child.setGroupBys(cGroups);
    }

    private static void mergeHaving(PlanNode parent, PlanNode child) {
        Item pHaving = parent.getHavingFilter();
        Item pHaving0 = PlanUtil.pushDownItem(parent, pHaving, true);
        Item mHaving = FilterUtils.and(pHaving0, child.getHavingFilter());
        child.having(mHaving);
    }

    private static void mergeOrderBy(PlanNode parent, PlanNode child) {
        List<Order> pOrders = parent.getOrderBys();
        List<Order> cOrders = new ArrayList<Order>();
        for (Order pOrder : pOrders) {
            Item col = pOrder.getItem();
            Item col0 = PlanUtil.pushDownItem(parent, col, true);
            Order pOrder0 = new Order(col0, pOrder.getSortOrder());
            cOrders.add(pOrder0);
        }
        if (cOrders.size() > 0)
            child.setOrderBys(cOrders);
    }

    private static void mergeLimit(PlanNode parent, PlanNode child) {
        child.setLimitFrom(parent.getLimitFrom());
        child.setLimitTo(parent.getLimitTo());
    }

}
