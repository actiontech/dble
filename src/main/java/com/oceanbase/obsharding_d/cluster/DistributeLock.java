/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster;

public abstract class DistributeLock {

    protected String path;
    protected String value;

    public abstract boolean acquire();

    public abstract void release();

    public String getPath() {
        return path;
    }
}
