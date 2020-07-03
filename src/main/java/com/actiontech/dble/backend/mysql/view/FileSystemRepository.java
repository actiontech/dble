/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.view;

import com.actiontech.dble.config.model.SystemConfig;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by szf on 2017/10/12.
 */
public class FileSystemRepository implements Repository {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemRepository.class);
    private FileChannel rwChannel = null;
    private String baseDir;
    private String baseName;
    private Map<String, Map<String, String>> viewCreateSqlMap = new HashMap<String, Map<String, String>>();


    public FileSystemRepository() {
        init();
    }

    public FileSystemRepository(Map<String, Map<String, String>> map) {
        init();
        viewCreateSqlMap = map;
    }

    /**
     * init the file read & create the viewMap
     */
    public void init() {
        try {
            baseDir = SystemConfig.getInstance().getViewPersistenceConfBaseDir();
            baseName = SystemConfig.getInstance().getViewPersistenceConfBaseName();

            //Judge whether exist the basedir
            createBaseDir();
            //open a channel of the view config file
            RandomAccessFile randomAccessFile = new RandomAccessFile(baseDir + baseName, "rw");
            rwChannel = randomAccessFile.getChannel();

            viewCreateSqlMap = this.getObject();
        } catch (Exception e) {
            LOGGER.info("init view from file error make sure the file is correct :" + e.getMessage());
        }
    }

    @Override
    public void terminate() {
        try {
            if (rwChannel != null) {
                rwChannel.close();
            }
        } catch (Exception e) {
            LOGGER.info("close error");
        }
    }

    /**
     * only used by ucore view
     * save the view info into local
     * so the view can be use without ucore
     */
    public void saveMapToFile() {
        try {
            this.writeToFile(mapToJsonString(viewCreateSqlMap));
        } catch (Exception e) {
            LOGGER.warn("ucore view data put local fail:" + e.getMessage());
        }
    }

    /**
     * delete the view info from both fileSystem & memory
     *
     * @param schemaName
     * @param viewName
     */
    public void delete(String schemaName, String viewName) {
        try {
            Map<String, Map<String, String>> tmp = new HashMap<String, Map<String, String>>();
            for (Map.Entry<String, Map<String, String>> entry : viewCreateSqlMap.entrySet()) {
                if (entry.getKey().equals(schemaName)) {
                    Map<String, String> tmpSchemaMap = new HashMap<String, String>();
                    for (Map.Entry<String, String> schemaEntry : entry.getValue().entrySet()) {
                        if (!schemaEntry.getKey().equals(viewName.trim())) {
                            tmpSchemaMap.put(schemaEntry.getKey(), schemaEntry.getValue());
                        }
                    }
                    tmp.put(entry.getKey(), tmpSchemaMap);
                    break;
                }
                tmp.put(entry.getKey(), entry.getValue());
            }

            this.writeToFile(mapToJsonString(tmp));

            Map<String, String> schemaMap = viewCreateSqlMap.get(schemaName);
            if (schemaMap == null) {
                schemaMap = new HashMap<String, String>();
                viewCreateSqlMap.put(schemaName, schemaMap);
            }
            schemaMap.remove(viewName.trim());
        } catch (Exception e) {
            LOGGER.warn("delete view from file error make sure the file is correct :" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * put the view info from both fileSystem & memory
     *
     * @param schemaName
     * @param viewName
     * @param createSql
     */
    public void put(String schemaName, String viewName, String createSql) {
        try {
            Map<String, Map<String, String>> tmp = new HashMap<String, Map<String, String>>();
            for (Map.Entry<String, Map<String, String>> entry : viewCreateSqlMap.entrySet()) {
                if (entry.getKey().equals(schemaName)) {
                    Map<String, String> tmpSchemaMap = new HashMap<String, String>();
                    for (Map.Entry<String, String> schemaEntry : entry.getValue().entrySet()) {
                        tmpSchemaMap.put(schemaEntry.getKey(), schemaEntry.getValue());
                    }
                    tmpSchemaMap.put(viewName, createSql);
                    tmp.put(entry.getKey(), tmpSchemaMap);
                    break;
                }
                tmp.put(entry.getKey(), entry.getValue());
            }

            Map<String, String> temSchemaMap = tmp.get(schemaName);
            if (temSchemaMap == null) {
                temSchemaMap = new HashMap<String, String>();
                tmp.put(schemaName, temSchemaMap);
                temSchemaMap.put(viewName, createSql);

            }


            this.writeToFile(mapToJsonString(tmp));
            Map<String, String> schemaMap = viewCreateSqlMap.get(schemaName);
            if (schemaMap == null) {
                schemaMap = new HashMap<String, String>();
                viewCreateSqlMap.put(schemaName, schemaMap);
            }
            schemaMap.put(viewName, createSql);


        } catch (Exception e) {
            LOGGER.warn("add view from file error make sure the file is correct :" + e.getMessage());
            throw new RuntimeException("put view data to file error", e);
        }

    }

    /**
     * read the json file and transform into memory
     *
     * @return
     * @throws Exception
     */
    public Map<String, Map<String, String>> getObject() throws Exception {
        Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
        String jsonString = readFromFile();
        JsonArray jsonArray = new JsonParser().parse(jsonString).getAsJsonArray();
        if (jsonArray != null) {
            for (JsonElement schema : jsonArray) {
                JsonObject x = schema.getAsJsonObject();
                String schemaName = x.get("schema").getAsString();
                JsonArray viewList = x.get("list").getAsJsonArray();
                Map<String, String> schemaView = new HashMap<String, String>();
                for (JsonElement view : viewList) {
                    JsonObject y = view.getAsJsonObject();
                    schemaView.put(y.get("name").getAsString(), y.get("sql").getAsString());
                }
                result.put(schemaName, schemaView);
            }
        }
        return result;
    }

    /**
     * just truncate the file and writeDirectly new info into
     *
     * @param jsonString
     * @throws Exception
     */
    public void writeToFile(String jsonString) throws Exception {
        ByteBuffer buff = ByteBuffer.wrap(jsonString.getBytes());
        rwChannel.truncate(0);
        rwChannel.write(buff);
        rwChannel.force(true);
    }

    /**
     * read form json file
     *
     * @return
     * @throws Exception
     */
    public String readFromFile() throws Exception {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(baseDir + baseName);
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);
            StringBuilder sb = new StringBuilder("");
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } catch (Exception e) {
            LOGGER.error("can not read file from viewFile", e);
        } finally {
            if (br != null) {
                br.close();
            }
            if (isr != null) {
                isr.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
        return null;
    }


    /**
     * create the log base dir
     */
    private void createBaseDir() {
        File baseDirFolder = new File(baseDir);
        if (!baseDirFolder.exists()) {
            baseDirFolder.mkdirs();
        }
    }


    /**
     * transform the viewMap into JsonString
     *
     * @return
     */
    public String mapToJsonString(Map<String, Map<String, String>> map) {
        StringBuilder sb = new StringBuilder("[");
        for (Map.Entry<String, Map<String, String>> schema : map.entrySet()) {
            Map<String, String> schemaSet = schema.getValue();
            sb.append("{\"schema\":\"").append(schema.getKey()).append("\",\"list\":[");
            for (Map.Entry<String, String> view : schemaSet.entrySet()) {
                sb.append("{\"name\":\"").append(view.getKey()).append("\",\"sql\":\"").append(view.getValue()).append("\"},");
            }
            if (',' == sb.charAt(sb.length() - 1)) {
                sb.setCharAt(sb.length() - 1, ']');
            }
            if (schemaSet.size() == 0) {
                sb.append("]");
            }
            sb.append("},");
        }
        if (',' == sb.charAt(sb.length() - 1)) {
            sb.setCharAt(sb.length() - 1, ']');
        }
        return sb.toString();
    }

    public Map<String, Map<String, String>> getViewCreateSqlMap() {
        return viewCreateSqlMap;
    }

    public void setViewCreateSqlMap(Map<String, Map<String, String>> viewCreateSqlMap) {
        this.viewCreateSqlMap = viewCreateSqlMap;
    }

}
