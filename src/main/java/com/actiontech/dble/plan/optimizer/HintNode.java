/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.optimizer;

/**
 * @author dcy
 * Create Date: 2021-12-02
 */
public final class HintNode {
    String name;

    private HintNode(String name) {
        this.name = name;
    }

    public static HintNode of(String name) {
        return new HintNode(name);
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "nodeName='" + name + '\'';
    }
}
