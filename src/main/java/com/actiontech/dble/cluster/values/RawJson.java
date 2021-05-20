/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.values;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.util.Strings;

/**
 * @author dcy
 * Create Date: 2021-04-06
 */
public abstract class RawJson {
    private RawJson() {
    }

    public abstract String getRaw();

    public abstract JsonObject getJsonObject();


    @Deprecated
    public static RawJson of(String data) {
        if (Strings.isBlank(data)) {
            return new RawJsonForStr(null);
        }
        return new RawJsonForStr(data);
    }


    public static RawJson of(JsonObject data) {
        if (data == null) {
            return new RawJsonForJson(null);
        }
        return new RawJsonForJson(data);
    }

    private static final class RawJsonForStr extends RawJson {
        String data;

        private RawJsonForStr(String data) {
            this.data = data;
        }

        @Override
        public String getRaw() {
            return data;
        }

        @Override
        public JsonObject getJsonObject() {
            return data == null ? null : (JsonObject) new JsonParser().parse(data);
        }

        @Override
        public String toString() {
            return getRaw();
        }
    }

    private static final class RawJsonForJson extends RawJson {
        JsonObject data;
        String dataTmpStr;

        private RawJsonForJson(JsonObject data) {
            this.data = data;
        }

        @Override
        public String getRaw() {
            if (dataTmpStr != null) {
                return dataTmpStr;
            }
            return data == null ? null : (dataTmpStr = data.toString());
        }

        @Override
        public JsonObject getJsonObject() {
            return data;
        }

        @Override
        public String toString() {
            return getRaw();
        }
    }


}
