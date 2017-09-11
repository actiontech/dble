/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.node.view;

import com.actiontech.dble.plan.PlanNode;
import com.actiontech.dble.plan.util.PlanUtil;

/**
 * Some function from mysql source code
 *
 * @author ActionTech
 */
public final class ViewUtil {
    private ViewUtil() {
    }

    /**
     * Check whether the merging algorithm can be used on this VIEW see
     * LEX::can_be_merged() in sql_lex.cc
     *
     * @param viewSelNode view's real selectnode
     * @return FALSE - only temporary table algorithm can be used TRUE - merge
     * algorithm can be used
     */
    public static boolean canBeMerged(PlanNode viewSelNode) {
        if (viewSelNode.type() == PlanNode.PlanNodeType.NONAME)
            return true;
        boolean selectsAllowMerge = viewSelNode.type() != PlanNode.PlanNodeType.MERGE;
        // TODO as the same as LEX::can_be_merged();
        boolean existAggr = PlanUtil.existAggr(viewSelNode);
        return selectsAllowMerge && viewSelNode.getReferedTableNodes().size() >= 1 && !existAggr;
    }
}
