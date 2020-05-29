/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

public abstract class DistributeLock {

    protected String path;
    protected String value;
    public abstract boolean acquire();
    public abstract void release();

    public String getPath() {
        return path;
    }
}
