/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.plan.optimizer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author collapsar
 */
public final class HintPlanNodeGroup {

    List<HintPlanNode> nodes = new ArrayList<>();
    Type type;

    private HintPlanNodeGroup(Type type, String... names) {
        for (String name : names) {
            nodes.add(HintPlanNode.of(name));
        }
        this.type = type;
    }

    private HintPlanNodeGroup(Type type, List<String> names) {
        for (String name : names) {
            nodes.add(HintPlanNode.of(name));
        }
        this.type = type;
    }

    public static HintPlanNodeGroup of(Type type, String... name) {
        return new HintPlanNodeGroup(type, name);
    }

    public static HintPlanNodeGroup of(Type type, List<String> name) {
        return new HintPlanNodeGroup(type, name);
    }


    public Type getType() {
        return type;
    }

    public List<HintPlanNode> getNodes() {
        return nodes;
    }

    @Override
    public String toString() {
        return "nodes='" + nodes + "',type is " + type.toString();
    }

    public enum Type {
        ER, AND, OR
    }
}
