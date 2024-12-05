/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster;

import com.oceanbase.obsharding_d.cluster.values.ClusterValueAdapterForRead;
import com.oceanbase.obsharding_d.cluster.values.ClusterValueAdapterForWrite;
import com.oceanbase.obsharding_d.cluster.values.ClusterValueForBaseWrite;
import com.oceanbase.obsharding_d.cluster.values.ClusterValueForRead;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.sharding.schema.Table;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.sharding.schema.TableGsonAdapter;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.user.User;
import com.oceanbase.obsharding_d.cluster.zkprocess.entity.user.UserGsonAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public final class JsonFactory {
    private static final Gson MAPPER = new GsonBuilder().registerTypeAdapter(Table.class, new TableGsonAdapter()).registerTypeAdapter(User.class, new UserGsonAdapter()).registerTypeAdapter(ClusterValueForRead.class, new ClusterValueAdapterForRead()).registerTypeHierarchyAdapter(ClusterValueForBaseWrite.class, new ClusterValueAdapterForWrite()).create();

    private JsonFactory() {
    }

    public static Gson getJson() {
        return MAPPER;
    }
}
