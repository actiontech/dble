/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.logic;

/**
 * @author dcy
 * Create Date: 2021-03-17
 */
public enum ClusterOperation {
    PAUSE_RESUME(1),
    VIEW(1),


    HA(1),
    CONFIG(1),

    META(1),
    DDL(1),
    UNKNOWN(1),
    BINGLOG(1),
    ONLINE(1),
    ;

    private int apiVersion;

    ClusterOperation(int apiVersion) {
        this.apiVersion = apiVersion;
    }

    public int getApiVersion() {
        return apiVersion;
    }
}
