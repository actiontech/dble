/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.response;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.comm.ConfFileRWUtils;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by szf on 2018/1/29.
 */
public class PropertySequenceLoader implements ClusterXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertySequenceLoader.class);

    private static final String PROPERTIES_SUFFIX = ".properties";

    private static final String PROPERTIES_SEQUENCE_CONF = "sequence_conf";

    private static final String PROPERTIES_SEQUENCE_DB_CONF = "sequence_db_conf";


    private static final String CONFIG_PATH = ClusterPathUtil.getSequencesPath();


    public PropertySequenceLoader(ClusterClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getConfChangeLockPath());
        if (ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID).equals(lock.getValue())) {
            return;
        }
        if (configValue.getValue() != null && !"".equals(configValue.getValue())) {
            JSONObject jsonObj = JSONObject.parseObject(configValue.getValue());
            if (jsonObj.get(PROPERTIES_SEQUENCE_CONF) != null) {
                String sequenceConf = jsonObj.getString(PROPERTIES_SEQUENCE_CONF);
                ConfFileRWUtils.writeFile(PROPERTIES_SEQUENCE_CONF + PROPERTIES_SUFFIX, sequenceConf);
            }

            if (jsonObj.get(PROPERTIES_SEQUENCE_DB_CONF) != null) {
                String sequenceConf = jsonObj.getString(PROPERTIES_SEQUENCE_DB_CONF);
                ConfFileRWUtils.writeFile(PROPERTIES_SEQUENCE_DB_CONF + PROPERTIES_SUFFIX, sequenceConf);
            }
        }
    }

    @Override
    public void notifyCluster() throws Exception {
        JSONObject jsonObject = new JSONObject();
        String sequenceConf = ConfFileRWUtils.readFileWithOutError(PROPERTIES_SEQUENCE_CONF + PROPERTIES_SUFFIX);
        if (sequenceConf != null && !"".equals(sequenceConf)) {
            jsonObject.put(PROPERTIES_SEQUENCE_CONF, sequenceConf);
        }
        String sequenceDbConf = ConfFileRWUtils.readFileWithOutError(PROPERTIES_SEQUENCE_DB_CONF + PROPERTIES_SUFFIX);
        if (sequenceDbConf != null && !"".equals(sequenceDbConf)) {
            jsonObject.put(PROPERTIES_SEQUENCE_DB_CONF, sequenceDbConf);
        }
        ClusterHelper.setKV(CONFIG_PATH, jsonObject.toJSONString());
    }

}
