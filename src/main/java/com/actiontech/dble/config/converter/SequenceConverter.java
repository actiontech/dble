/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.converter;

import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.route.util.OrderedProperties;
import com.actiontech.dble.route.util.PropertiesUtil;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;
import java.util.Properties;

public class SequenceConverter {

    private String fileName;

    public static RawJson sequencePropsToJson(String fileName) {
        JsonObject jsonObject = new JsonObject();
        Map<String, String> properties = PropertiesUtil.getOrderedMap(fileName);
        jsonObject.addProperty(fileName, (new Gson()).toJson(properties));
        return RawJson.of(jsonObject);
    }

    public Properties jsonToProperties(RawJson sequenceJson) {
        JsonObject jsonObj = sequenceJson.getJsonObject();
        Map.Entry<String, JsonElement> sequenceEntry = jsonObj.entrySet().iterator().next();
        if (null == sequenceEntry) {
            return null;
        }
        this.fileName = sequenceEntry.getKey();
        JsonElement fileContent = sequenceEntry.getValue();
        JsonObject fileContentJson = new JsonParser().parse(fileContent.getAsString()).getAsJsonObject();
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
