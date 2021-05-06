/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import javax.annotation.Nonnull;

/**
 * @author dcy
 * Create Date: 2021-03-30
 */
public class ClusterEvent<T> {
    private String path;
    private ClusterValue<T> value;
    private ChangeType changeType;

    public ClusterEvent(@Nonnull String path, @Nonnull ClusterValue<T> value, @Nonnull ChangeType changeType) {
        this.path = path;
        this.value = value;
        this.changeType = changeType;
    }

    @Nonnull
    public String getPath() {
        return path;
    }

    @Nonnull
    public ClusterValue<T> getValue() {
        return value;
    }

    @Nonnull
    public ChangeType getChangeType() {
        return changeType;
    }


    @Override
    public String toString() {
        return "ClusterEvent{" +
                "path='" + path + '\'' +
                ", value=" + value +
                ", changeType=" + changeType +
                '}';
    }
}
