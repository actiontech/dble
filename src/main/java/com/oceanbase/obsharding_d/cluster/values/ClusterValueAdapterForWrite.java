/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

import com.oceanbase.obsharding_d.cluster.JsonFactory;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * @author dcy
 * Create Date: 2021-04-02
 */
public class ClusterValueAdapterForWrite implements JsonSerializer<ClusterValueForBaseWrite<?>> {
    @Override
    public JsonElement serialize(ClusterValueForBaseWrite<?> value, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject obj = new JsonObject();

        obj.addProperty("instanceName", value.getInstanceName());
        obj.addProperty("apiVersion", value.apiVersion);
        obj.addProperty("createdAt", value.createdAt);
        if (value.data != null) {
            if (value.data instanceof SelfSerialize) {
                //custom serialize
                obj.add("data", ((SelfSerialize<?>) value.data).serialize());
            } else if (value.data instanceof String) {
                obj.addProperty("data", (String) value.data);
            } else {
                obj.add("data", JsonFactory.getJson().toJsonTree(value.data));
            }
        } else if (value.rawData != null) {
            obj.add("data", value.rawData);
        } else {
            throw new IllegalStateException("illegal use for cluster value.Data can't be null, maybe you need use class 'Empty'");
        }


        return obj;
    }
}
