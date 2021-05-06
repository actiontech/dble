/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.google.gson.JsonElement;

/**
 * @author dcy
 * Create Date: 2021-03-17
 */
public class ClusterValueForRawWrite<T> extends ClusterValueForBaseWrite<T> {


    public ClusterValueForRawWrite(JsonElement data, int apiVersion) {
        super(data, apiVersion);
    }

}
