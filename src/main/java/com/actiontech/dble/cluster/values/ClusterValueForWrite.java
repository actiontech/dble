/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.values;

/**
 * @author dcy
 * Create Date: 2021-03-17
 */
public class ClusterValueForWrite<T> extends ClusterValueForBaseWrite<T> {


    public ClusterValueForWrite(T data, int apiVersion) {
        super(data, apiVersion);
    }
}
