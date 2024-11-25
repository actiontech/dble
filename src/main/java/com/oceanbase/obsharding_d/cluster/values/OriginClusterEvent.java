/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

import javax.annotation.Nonnull;

/**
 * @author dcy
 * Create Date: 2021-03-30
 */
public class OriginClusterEvent<T> {
    private String path;
    private ClusterValue<T> value;
    private ClusterValue<T> oldValue;
    private OriginChangeType changeType;


    public OriginClusterEvent(@Nonnull String path, @Nonnull ClusterValue<T> value, @Nonnull OriginChangeType changeType) {
        this.path = path;
        this.value = value;
        this.changeType = changeType;
    }

    public OriginClusterEvent(@Nonnull String path, @Nonnull ClusterValue<T> currentValue, @Nonnull ClusterValue<T> oldValue, @Nonnull OriginChangeType changeType) {
        this.path = path;
        this.value = currentValue;
        this.oldValue = oldValue;
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

    public ClusterValue<T> getOldValue() {
        return oldValue;
    }

    @Nonnull
    public OriginChangeType getChangeType() {
        return changeType;
    }

    @Override
    public String toString() {
        return "OriginClusterEvent{" +
                "path='" + path + '\'' +
                ", value=" + value +
                ", oldValue=" + oldValue +
                ", changeType=" + changeType +
                '}';
    }
}
