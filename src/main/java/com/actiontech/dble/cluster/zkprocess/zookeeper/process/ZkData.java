/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zookeeper.process;

import java.util.HashMap;
import java.util.Map;

public class ZkData {
    private String name;
    private String value;
    private Map<String, ZkData> children = new HashMap<>();

    public ZkData(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public Map<String, ZkData> getChildren() {
        return children;
    }

    public void addChild(ZkData child) {
        children.put(child.getName(), child);
    }

}
