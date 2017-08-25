package io.mycat.plan.optimizer;

import io.mycat.plan.PlanNode;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.util.FilterUtils;

import java.util.ArrayList;
import java.util.List;

public final class MergeHavingFilter {
    private MergeHavingFilter() {
    }

    /**
     * 将having中可合并的条件合并到where
     *
     * @param qtn
     */
    public static void optimize(PlanNode qtn) {
        if (qtn.getHavingFilter() != null) {
            List<Item> subFilters = FilterUtils.splitFilter(qtn.getHavingFilter());
            List<Item> canMergeSubs = new ArrayList<>();
            for (Item subFilter : subFilters) {
                if (!subFilter.isWithSumFunc()) {
                    canMergeSubs.add(subFilter);
                }
            }
            subFilters.removeAll(canMergeSubs);
            qtn.having(FilterUtils.and(subFilters));
            qtn.setWhereFilter(FilterUtils.and(qtn.getWhereFilter(), FilterUtils.and(canMergeSubs)));
        }
        for (PlanNode child : qtn.getChildren())
            optimize(child);
    }
}
