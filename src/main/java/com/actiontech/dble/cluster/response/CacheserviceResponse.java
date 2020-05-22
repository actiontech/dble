/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.response;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.comm.ConfFileRWUtils;
import com.actiontech.dble.config.model.SystemConfig;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/5/16.
 */
public class CacheserviceResponse implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheserviceResponse.class);

    private static final String PROPERTIES_SUFFIX = ".properties";

    private static final String PROPERTIES_CACHESERVER_NAME = "cacheservice";

    private static final String CONFIG_PATH = ClusterPathUtil.getEhcacheProPath();

    public CacheserviceResponse(ClusterClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getConfChangeLockPath());
        if (SystemConfig.getInstance().getInstanceId().equals(lock.getValue())) {
            return;
        }
        if (configValue.getValue() != null && !"".equals(configValue.getValue())) {
            JsonObject jsonObj = new JsonParser().parse(configValue.getValue()).getAsJsonObject();
            if (jsonObj.get(PROPERTIES_CACHESERVER_NAME) != null) {
                String sequenceConf = jsonObj.get(PROPERTIES_CACHESERVER_NAME).getAsString();
                ConfFileRWUtils.writeFile(PROPERTIES_CACHESERVER_NAME + PROPERTIES_SUFFIX, sequenceConf);
            }
        }
    }

    @Override
    public void notifyCluster() throws Exception {
        JsonObject jsonObject = new JsonObject();
        String cacheService = ConfFileRWUtils.readFileWithOutError(PROPERTIES_CACHESERVER_NAME + PROPERTIES_SUFFIX);
        if (!"".equals(cacheService)) {
            jsonObject.addProperty(PROPERTIES_CACHESERVER_NAME, cacheService);
        }
        ClusterHelper.setKV(CONFIG_PATH, jsonObject.getAsString());
    }
}
