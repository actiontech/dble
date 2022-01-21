/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.converter;

import com.actiontech.dble.cluster.values.JsonObjectWriter;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.route.util.OrderedProperties;
import com.actiontech.dble.route.util.PropertiesUtil;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Map;
import java.util.Properties;

public class SequenceConverter {

    private String fileName;

    public static RawJson sequencePropsToJson(String fileName) {
        JsonObjectWriter jsonObject = new JsonObjectWriter();
        JsonObject properties = PropertiesUtil.getOrderedMap(fileName);
        jsonObject.add(fileName, properties);
        return RawJson.of(jsonObject);
    }

    public Properties jsonToProperties(RawJson sequenceJson) {
        JsonObject jsonObj = sequenceJson.getJsonObject();
        //must only one entry
        Map.Entry<String, JsonElement> sequenceEntry = jsonObj.entrySet().iterator().next();
        if (null == sequenceEntry) {
            return null;
        }
        this.fileName = sequenceEntry.getKey();
        JsonObject fileContentJson = (JsonObject) sequenceEntry.getValue();
        Properties props = new OrderedProperties();
        fileContentJson.entrySet().forEach(jsonEntry -> props.setProperty(jsonEntry.getKey(), jsonEntry.getValue().getAsString()));
        return props;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
