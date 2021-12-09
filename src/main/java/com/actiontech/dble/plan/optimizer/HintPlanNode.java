/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.plan.optimizer;

import java.util.Arrays;

/**
 * @author collapsar
 */
public final class HintPlanNode {

    String[] name;
    Type type;

    private HintPlanNode(Type type, String... name) {
        this.name = name;
        this.type = type;
    }

    public static HintPlanNode of(Type type, String... name) {
        return new HintPlanNode(type, name);
    }

    public String[] getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return "nodeName='" + Arrays.toString(name) + "',type is " + type.toString();
    }

    public enum Type {
        ER, AND, OR
    }
}
