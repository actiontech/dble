/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.view;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.log.alarm.AlarmCode;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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


    /**
     * init the file read & create the viewMap
     */
    public void init() {
        try {
            ServerConfig config = DbleServer.getInstance().getConfig();
            SystemConfig systemConfig = config.getSystem();

            baseDir = systemConfig.getViewPersistenceConfBaseDir();
            baseName = systemConfig.getViewPersistenceConfBaseName();

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

    /**
     * delete the view info from both fileSystem & memory
     *
     * @param schemaName
     * @param viewName
     */
    public void delete(String schemaName, String[] viewName) {
        try {
            for (String singleName : viewName) {
                Map<String, String> schemaMap = viewCreateSqlMap.get(schemaName);
                if (schemaMap == null) {
                    schemaMap = new HashMap<String, String>();
                    viewCreateSqlMap.put(schemaName, schemaMap);
                }
                schemaMap.remove(singleName.trim());
            }
            this.writeToFile(mapToJsonString());
        } catch (Exception e) {
            LOGGER.warn(AlarmCode.CORE_FILE_WRITE_WARN + "delete view from file error make sure the file is correct :" + e.getMessage());
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
            Map<String, String> schemaMap = viewCreateSqlMap.get(schemaName);
            if (schemaMap == null) {
                schemaMap = new HashMap<String, String>();
                viewCreateSqlMap.put(schemaName, schemaMap);
            }
            schemaMap.put(viewName, createSql);

            this.writeToFile(mapToJsonString());
        } catch (Exception e) {
            LOGGER.warn(AlarmCode.CORE_FILE_WRITE_WARN + "add view from file error make sure the file is correct :" + e.getMessage());
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
        JSONArray jsonArray = JSONObject.parseArray(jsonString);
        if (jsonArray != null) {
            for (Object schema : jsonArray) {
                JSONObject x = (JSONObject) schema;
                String schemaName = x.getString("schema");
                JSONArray viewList = x.getJSONArray("list");
                Map<String, String> schemaView = new HashMap<String, String>();
                for (Object view : viewList) {
                    JSONObject y = (JSONObject) view;
                    schemaView.put(y.getString("name"), y.getString("sql"));
                }
                result.put(schemaName, schemaView);
            }
        }
        return result;
    }

    /**
     * just truncate the file and write new info into
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
        FileInputStream fis = new FileInputStream(baseDir + baseName);
        InputStreamReader isr = new InputStreamReader(fis);
        BufferedReader br = new BufferedReader(isr);
        StringBuilder sb = new StringBuilder("");
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        br.close();
        return sb.toString();
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
    public String mapToJsonString() {
        StringBuilder sb = new StringBuilder("[");
        for (Map.Entry<String, Map<String, String>> schema : viewCreateSqlMap.entrySet()) {
            Map<String, String> schemaSet = schema.getValue();
            sb.append("{\"schema\":\"").append(schema.getKey()).append("\",\"list\":[");
            for (Map.Entry<String, String> view : schemaSet.entrySet()) {
                sb.append("{\"name\":\"").append(view.getKey()).append("\",\"sql\":\"").append(view.getValue()).append("\"},");
            }
            if (',' == sb.charAt(sb.length() - 1)) {
                sb.setCharAt(sb.length() - 1, ']');
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
