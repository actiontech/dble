/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

/**
 * @author dcy
 * Create Date: 2021-03-17
 */
public class ClusterValueForWrite<T> extends ClusterValueForBaseWrite<T> {


    public ClusterValueForWrite(T data, int apiVersion) {
        super(data, apiVersion);
    }
}
