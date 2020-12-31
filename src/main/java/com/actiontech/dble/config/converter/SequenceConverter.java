package com.actiontech.dble.config.converter;

import com.actiontech.dble.route.util.PropertiesUtil;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Map;
import java.util.Properties;

public class SequenceConverter {

    private String fileName;

    public static String sequencePropsToJson(String fileName) {
        JsonObject jsonObject = new JsonObject();
        Properties properties = PropertiesUtil.loadProps(fileName);
        jsonObject.addProperty(fileName, (new Gson()).toJson(properties));
        return (new Gson()).toJson(jsonObject);
    }

    public Properties jsonToProperties(String sequenceJson) {
        JsonObject jsonObj = new JsonParser().parse(sequenceJson).getAsJsonObject();
        Map.Entry<String, JsonElement> sequenceEntry = jsonObj.entrySet().iterator().next();
        if (null == sequenceEntry) {
            return null;
        }
        this.fileName = sequenceEntry.getKey();
        JsonElement fileContent = sequenceEntry.getValue();
        JsonObject fileContentJson = new JsonParser().parse(fileContent.getAsString()).getAsJsonObject();
        Properties props = new Properties();
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
