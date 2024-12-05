/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

import javax.annotation.Nonnull;

/**
 * @author dcy
 * Create Date: 2021-03-30
 */
public class ClusterEvent<T> {
    private String path;
    private ClusterValue<T> value;
    private ChangeType changeType;
    /**
     * it is update event before.
     * used for distinguish ADD event and UPDATE event.
     */
    private boolean update = false;

    public ClusterEvent(@Nonnull String path, @Nonnull ClusterValue<T> value, @Nonnull ChangeType changeType) {
        this.path = path;
        this.value = value;
        this.changeType = changeType;
    }

    public boolean isUpdate() {
        return update;
    }

    public ClusterEvent<T> markUpdate() {
        this.update = true;
        return this;
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
                ", update=" + update +
                '}';
    }
}
