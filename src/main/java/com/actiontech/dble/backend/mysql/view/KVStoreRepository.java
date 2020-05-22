/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.view;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.actiontech.dble.util.KVPathUtil.SEPARATOR;

/**
 * Created by szf on 2017/10/12.
 */
public class KVStoreRepository implements Repository {
    private static final Logger LOGGER = LoggerFactory.getLogger(KVStoreRepository.class);
    private Map<String, Map<String, String>> viewCreateSqlMap = null;
    private CuratorFramework zkConn = ZKUtils.getConnection();

    public KVStoreRepository() {
        this.init();
    }

    public void init() {
        Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
        try {
            List<String> viewList = zkConn.getChildren().forPath(KVPathUtil.getViewPath());
            for (String singlePath : viewList) {
                String[] paths = singlePath.split("/");
                String jsonData = new String(zkConn.getData().forPath(KVPathUtil.getViewPath() + SEPARATOR + singlePath), "UTF-8");
                JsonObject obj = new JsonParser().parse(jsonData).getAsJsonObject();

                String createSql = obj.get(CREATE_SQL).getAsString();
                String schema = paths[paths.length - 1].split(SCHEMA_VIEW_SPLIT)[0];
                String viewName = paths[paths.length - 1].split(SCHEMA_VIEW_SPLIT)[1];
                map.computeIfAbsent(schema, k -> new HashMap<>());
                map.get(schema).put(viewName, createSql);
            }
        } catch (Exception e) {
            LOGGER.info("init viewData from zk error :　" + e.getMessage());
        } finally {
            viewCreateSqlMap = map;
        }
    }

    @Override
    public void terminate() {

    }

    @Override
    public Map<String, Map<String, String>> getViewCreateSqlMap() {
        return viewCreateSqlMap;
    }

    @Override
    public void put(String schemaName, String viewName, String createSql) {
        StringBuffer sb = new StringBuffer(KVPathUtil.getViewPath()).append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(viewName);
        JsonObject m = new JsonObject();
        m.addProperty(SERVER_ID, SystemConfig.getInstance().getInstanceId());
        m.addProperty(CREATE_SQL, createSql);
        try {
            if (zkConn.checkExists().forPath(sb.toString()) == null) {
                zkConn.create().forPath(sb.toString(), m.getAsString().getBytes());
            } else {
                zkConn.setData().forPath(sb.toString(), m.getAsString().getBytes());
            }
        } catch (Exception e) {
            LOGGER.warn("create zk node error :　" + e.getMessage());
        }

    }

    /**
     * @param schemaName
     * @param view
     */
    @Override
    public void delete(String schemaName, String view) {
        StringBuffer sb = new StringBuffer(KVPathUtil.getViewPath()).append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(view);
        try {
            zkConn.delete().forPath(sb.toString());
        } catch (Exception e) {
            LOGGER.warn("delete zk node error :　" + e.getMessage());
        }
    }
}
