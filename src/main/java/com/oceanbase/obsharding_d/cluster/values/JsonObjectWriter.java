/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.Serializable;

/**
 * @author dcy
 * Create Date: 2021-05-21
 */
public class JsonObjectWriter implements Serializable {
    private final transient JsonObject jsonObject = new JsonObject();
    private static final long serialVersionUID = 1L;

    public void add(String property, JsonElement value) {
        if (value == null || value.isJsonNull()) {
            //ignore null
            return;
        }
        jsonObject.add(property, value);
    }


    public void addProperty(String property, String value) {
        if (value == null) {
            //ignore null
            return;
        }
        jsonObject.addProperty(property, value);
    }

    public void addProperty(String property, Number value) {
        if (value == null) {
            //ignore null
            return;
        }
        jsonObject.addProperty(property, value);
    }

    public void addProperty(String property, Boolean value) {
        if (value == null) {
            //ignore null
            return;
        }
        jsonObject.addProperty(property, value);
    }

    public void addProperty(String property, Character value) {
        if (value == null) {
            //ignore null
            return;
        }
        jsonObject.addProperty(property, value);
    }


    public int size() {
        return jsonObject.size();
    }


    public JsonObject toJsonObject() {
        return jsonObject;
    }
}
