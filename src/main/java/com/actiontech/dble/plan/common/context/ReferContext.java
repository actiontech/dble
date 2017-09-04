package com.actiontech.dble.plan.common.context;

import com.actiontech.dble.plan.PlanNode;

public class ReferContext {

    private PlanNode planNode;
    private boolean isPushDownNode;

    public ReferContext() {
        this.planNode = null;
        this.isPushDownNode = false;
    }

    public PlanNode getPlanNode() {
        return planNode;
    }

    public void setPlanNode(PlanNode planNode) {
        this.planNode = planNode;
    }

    public boolean isPushDownNode() {
        return isPushDownNode;
    }

    public void setPushDownNode(boolean pushDownNode) {
        this.isPushDownNode = pushDownNode;
    }

}
