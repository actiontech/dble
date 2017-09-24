/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.console;

/**
 * ZookeeperPath
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public enum ZookeeperPath {

    /**
     * the local path where zk will write to
     */
    ZK_LOCAL_WRITE_PATH("./"),;
    private String key;

    ZookeeperPath(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }


}
