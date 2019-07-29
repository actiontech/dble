/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.view;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import com.alibaba.fastjson.JSONObject;
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
                JSONObject obj = (JSONObject) JSONObject.parse(jsonData);

                String createSql = obj.getString(CREATE_SQL);
                String schema = paths[paths.length - 1].split(SCHEMA_VIEW_SPLIT)[0];
                String viewName = paths[paths.length - 1].split(SCHEMA_VIEW_SPLIT)[1];
                if (map.get(schema) == null) {
                    map.put(schema, new HashMap<String, String>());
                }
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
        JSONObject m = new JSONObject();
        m.put(SERVER_ID, ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
        m.put(CREATE_SQL, createSql);
        try {
            if (zkConn.checkExists().forPath(sb.toString()) == null) {
                zkConn.create().forPath(sb.toString(), m.toJSONString().getBytes());
            } else {
                zkConn.setData().forPath(sb.toString(), m.toJSONString().getBytes());
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
