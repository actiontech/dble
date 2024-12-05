/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.optimizer;

import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.util.FilterUtils;

import java.util.ArrayList;
import java.util.List;

public final class MergeHavingFilter {
    private MergeHavingFilter() {
    }

    /**
     * merge having to where if it can be merged
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
