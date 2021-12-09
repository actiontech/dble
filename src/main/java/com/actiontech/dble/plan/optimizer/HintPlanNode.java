/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.plan.optimizer;

/**
 * @author collapsar
 */
public final class HintPlanNode {

    String name;

    private HintPlanNode(String name) {
        this.name = name;
    }

    public static HintPlanNode of(String name) {
        return new HintPlanNode(name);
    }

    public String getName() {
        return name;
    }


    @Override
    public String toString() {
        return "nodeName='" + name + "'";
    }


}
