/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.context;

import com.actiontech.dble.plan.PlanNode;


public class NameResolutionContext {
    /*
     * The name resolution context to search in when an Item cannot be resolved
     * in this context (the context of an outer select)
     */
    NameResolutionContext outerContext;

    private PlanNode planNode;

    private boolean findInSelect = false;

    private boolean selectFirst = false;

    public NameResolutionContext getOuterContext() {
        return outerContext;
    }

    public void setOuterContext(NameResolutionContext outerContext) {
        this.outerContext = outerContext;
    }

    public PlanNode getPlanNode() {
        return planNode;
    }

    public void setPlanNode(PlanNode pn) {
        this.planNode = pn;
    }

    public boolean isFindInSelect() {
        return findInSelect;
    }

    public void setFindInSelect(boolean findInSelect) {
        this.findInSelect = findInSelect;
    }

    public boolean isSelectFirst() {
        return selectFirst;
    }

    public void setSelectFirst(boolean selectFirst) {
        this.selectFirst = selectFirst;
    }

}
