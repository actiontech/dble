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
public class ClusterEntry<T> {
    private String key;
    private ClusterValue<T> value;

    public ClusterEntry(String key, ClusterValue<T> value) {
        this.key = key;
        this.value = value;
    }

    @Nonnull
    public String getKey() {
        return key;
    }

    @Nonnull
    public ClusterValue<T> getValue() {
        return value;
    }
}
