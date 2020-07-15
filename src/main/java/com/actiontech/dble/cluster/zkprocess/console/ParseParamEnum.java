/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.console;

/**
 * ParseParamEnum
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/18
 */
public enum ParseParamEnum {

    /**
     * mapfile for rule
     */
    ZK_PATH_RULE_MAPFILE_NAME("mapFile"),;

    private String key;

    ParseParamEnum(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

}
