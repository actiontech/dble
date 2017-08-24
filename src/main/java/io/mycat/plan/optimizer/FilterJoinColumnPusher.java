package io.mycat.plan.optimizer;

import io.mycat.plan.PlanNode;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.ItemField;
import io.mycat.plan.common.item.function.operator.cmpfunc.ItemFuncEqual;
import io.mycat.plan.node.JoinNode;
import io.mycat.plan.node.QueryNode;
import io.mycat.plan.node.TableNode;
import io.mycat.plan.util.FilterUtils;
import io.mycat.plan.util.PlanUtil;
import io.mycat.route.parser.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 只下推有ER关系可能的filter
 */
public final class FilterJoinColumnPusher {
    private FilterJoinColumnPusher() {
    }


    public static PlanNode optimize(PlanNode qtn) {
        qtn = pushFilter(qtn, new ArrayList<Item>());
        return qtn;
    }

    private static PlanNode pushFilter(PlanNode qtn, List<Item> dnfNodeToPush) {
        // 如果是叶节点，接收filter做为where条件,否则继续合并当前where条件，然后下推
        if (qtn.getChildren().isEmpty()) {
            Item node = FilterUtils.and(dnfNodeToPush);
            if (node != null) {
                qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
            }
            return qtn;
        }

        Item filterInWhere = qtn.getWhereFilter();
        //left/right join: where filter can't be push to child
        if (filterInWhere != null && (!(qtn.type() == PlanNode.PlanNodeType.JOIN) || ((JoinNode) qtn).isInnerJoin())) {
            List<Item> splits = FilterUtils.splitFilter(filterInWhere);
            List<Item> nonJoinFilter = new ArrayList<Item>();
            for (Item filter : splits) {
                if (!isPossibleERJoinColumnFilter(qtn, filter)) {
                    nonJoinFilter.add(filter);
                } else {
                    dnfNodeToPush.add((ItemFuncEqual) filter);
                }
            }
            if (nonJoinFilter.size() != splits.size()) {
                //不可能join的条件放回
                qtn.query(FilterUtils.and(nonJoinFilter));
            }
        }
        PlanNode.PlanNodeType i = qtn.type();
        if (i == PlanNode.PlanNodeType.QUERY) {
            if (dnfNodeToPush.isEmpty()) {
                return qtn;
            }
            refreshPdFilters(qtn, dnfNodeToPush);
            PlanNode child = pushFilter(qtn.getChild(), dnfNodeToPush);
            ((QueryNode) qtn).setChild(child);

        } else if (i == PlanNode.PlanNodeType.JOIN) {
            JoinNode jn = (JoinNode) qtn;
            PlanUtil.findJoinKeysAndRemoveIt(dnfNodeToPush, jn);
            if (dnfNodeToPush.isEmpty()) {
                return qtn;
            }
            // 无法完成下推的filters
            List<Item> dnfNodeToCurrent = new LinkedList<Item>();
            List<Item> dnfNodetoPushToLeft = new LinkedList<Item>();
            List<Item> dnfNodetoPushToRight = new LinkedList<Item>();
            for (Item filter : dnfNodeToPush) {
                if (PlanUtil.canPush(filter, jn.getLeftNode(), jn)) {
                    dnfNodetoPushToLeft.add(filter);
                } else if (PlanUtil.canPush(filter, jn.getRightNode(), jn)) {
                    dnfNodetoPushToRight.add(filter);
                } else {
                    dnfNodeToCurrent.add(filter);
                }
            }
            // 针对不能下推的，合并到当前的where
            Item node = FilterUtils.and(dnfNodeToCurrent);
            if (node != null) {
                qtn.query(FilterUtils.and(qtn.getWhereFilter(), node));
            }
            if (jn.isInnerJoin()) {
                refreshPdFilters(jn, dnfNodetoPushToLeft);
                refreshPdFilters(jn, dnfNodetoPushToRight);
                pushFilter(jn.getLeftNode(), dnfNodetoPushToLeft);
                pushFilter(((JoinNode) qtn).getRightNode(), dnfNodetoPushToRight);
            } else if (jn.isLeftOuterJoin()) {
                refreshPdFilters(jn, dnfNodetoPushToLeft);
                pushFilter(jn.getLeftNode(), dnfNodetoPushToLeft);
                if (!dnfNodeToPush.isEmpty()) {
                    jn.query(FilterUtils.and(dnfNodetoPushToRight)); // 在父节点完成filter，不能下推
                }
            } else if (jn.isRightOuterJoin()) {
                refreshPdFilters(jn, dnfNodetoPushToRight);
                pushFilter(((JoinNode) qtn).getRightNode(), dnfNodetoPushToRight);
                if (!dnfNodeToPush.isEmpty()) {
                    jn.query(FilterUtils.and(dnfNodetoPushToLeft)); // 在父节点完成filter，不能下推
                }
            } else {
                if (!dnfNodeToPush.isEmpty()) {
                    jn.query(FilterUtils.and(dnfNodeToPush));
                }
            }

        } else if (i == PlanNode.PlanNodeType.MERGE) {
            List<PlanNode> children = qtn.getChildren();
            for (PlanNode aChildren : children) {
                pushFilter(aChildren, new ArrayList<Item>());
            }

        }
        return qtn;
    }

    /**
     * 是否是可能得ER关系Filter： 1.Filter必须是=关系 2.Filter必须是Column = Column
     * 3.Filter的key和value必须来自于不同的两张表 ex:a.id=b.id true a.id=b.id+1 false
     */
    private static boolean isPossibleERJoinColumnFilter(PlanNode node, Item ifilter) {
        if (!(ifilter instanceof ItemFuncEqual))
            return false;
        ItemFuncEqual filter = (ItemFuncEqual) ifilter;
        Item column = filter.arguments().get(0);
        Item value = filter.arguments().get(1);
        if (column != null && column instanceof ItemField && value != null && value instanceof ItemField) {
            Pair<TableNode, ItemField> foundColumn = PlanUtil.findColumnInTableLeaf((ItemField) column, node);
            Pair<TableNode, ItemField> foundValue = PlanUtil.findColumnInTableLeaf((ItemField) value, node);
            if (foundColumn != null && foundValue != null) {
                String columnTable = foundColumn.getValue().getTableName();
                String valueTable = foundValue.getValue().getTableName();
                // 不是同一张表才可以
                return !StringUtils.equals(columnTable, valueTable);
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    private static void refreshPdFilters(PlanNode qtn, List<Item> filters) {
        for (int index = 0; index < filters.size(); index++) {
            Item toPsFilter = filters.get(index);
            Item pdFilter = PlanUtil.pushDownItem(qtn, toPsFilter);
            filters.set(index, pdFilter);
        }
    }

}
