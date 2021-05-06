/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.values.*;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.Table;
import com.actiontech.dble.cluster.zkprocess.entity.sharding.schema.TableGsonAdapter;
import com.actiontech.dble.cluster.zkprocess.entity.user.User;
import com.actiontech.dble.cluster.zkprocess.entity.user.UserGsonAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public final class JsonFactory {
    private static final Gson MAPPER = new GsonBuilder().registerTypeAdapter(Table.class, new TableGsonAdapter()).registerTypeAdapter(User.class, new UserGsonAdapter()).registerTypeAdapter(ClusterValueForRead.class, new ClusterValueAdapterForRead()).registerTypeAdapter(ClusterValueForRawWrite.class, new ClusterValueAdapterForWrite()).registerTypeAdapter(ClusterValueForWrite.class, new ClusterValueAdapterForWrite()).create();

    private JsonFactory() {
    }

    public static Gson getJson() {
        return MAPPER;
    }
}
