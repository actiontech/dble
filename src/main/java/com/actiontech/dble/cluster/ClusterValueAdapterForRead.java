/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster;

import com.google.gson.*;

import java.lang.reflect.Type;

/**
 * @author dcy
 * Create Date: 2021-04-02
 */
public class ClusterValueAdapterForRead implements JsonDeserializer<ClusterValueForRead<?>> {


    @Override
    public ClusterValueForRead<?> deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonElement data = jsonObject.get("data");

        final int apiVersion = jsonObject.get("apiVersion").getAsInt();
        final long createdAt = jsonObject.get("createdAt").getAsLong();
        final String instanceName = jsonObject.get("instanceName").getAsString();
        final ClusterValueForRead<?> value = new ClusterValueForRead<>();
        value.rawData = data;
        value.createdAt = createdAt;
        value.apiVersion = apiVersion;
        value.instanceName = instanceName;
        return value;

    }
}
