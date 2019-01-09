/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreXmlLoader;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.comm.ConfFileRWUtils;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/5/16.
 */
public class UCacheserviceResponse implements UcoreXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(UCacheserviceResponse.class);

    private static final String PROPERTIES_SUFFIX = ".properties";

    private static final String PROPERTIES_CACHESERVER_NAME = "cacheservice";

    private static final String CONFIG_PATH = UcorePathUtil.getEhcacheProPath();

    public UCacheserviceResponse(UcoreClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        UKvBean lock = ClusterUcoreSender.getKey(UcorePathUtil.getConfChangeLockPath());
        if (UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID).equals(lock.getValue())) {
            return;
        }
        if (configValue.getValue() != null && !"".equals(configValue.getValue())) {
            JSONObject jsonObj = JSONObject.parseObject(configValue.getValue());
            if (jsonObj.get(PROPERTIES_CACHESERVER_NAME) != null) {
                String sequenceConf = jsonObj.getString(PROPERTIES_CACHESERVER_NAME);
                ConfFileRWUtils.writeFile(PROPERTIES_CACHESERVER_NAME + PROPERTIES_SUFFIX, sequenceConf);
            }
        }
    }

    @Override
    public void notifyCluster() throws Exception {
        JSONObject jsonObject = new JSONObject();
        String cacheService = ConfFileRWUtils.readFileWithOutError(PROPERTIES_CACHESERVER_NAME + PROPERTIES_SUFFIX);
        if (cacheService != null && !"".equals(cacheService)) {
            jsonObject.put(PROPERTIES_CACHESERVER_NAME, cacheService);
        }
        ClusterUcoreSender.sendDataToUcore(CONFIG_PATH, jsonObject.toJSONString());
    }
}
