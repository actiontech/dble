/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import com.actiontech.dble.cluster.zkprocess.comm.ConfFileRWUtils;
import com.actiontech.dble.config.model.SystemConfig;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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


    private static final String CONFIG_PATH = ClusterPathUtil.getSequencesCommonPath();


    public PropertySequenceLoader(ClusterClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getConfChangeLockPath());
        if (SystemConfig.getInstance().getInstanceName().equals(lock.getValue())) {
            return;
        }
        if (configValue.getValue() != null && !"".equals(configValue.getValue())) {
            JsonObject jsonObj = new JsonParser().parse(configValue.getValue()).getAsJsonObject();
            if (jsonObj.get(PROPERTIES_SEQUENCE_CONF) != null) {
                String sequenceConf = jsonObj.get(PROPERTIES_SEQUENCE_CONF).getAsString();
                ConfFileRWUtils.writeFile(PROPERTIES_SEQUENCE_CONF + PROPERTIES_SUFFIX, sequenceConf);
            }

            if (jsonObj.get(PROPERTIES_SEQUENCE_DB_CONF) != null) {
                String sequenceConf = jsonObj.get(PROPERTIES_SEQUENCE_DB_CONF).getAsString();
                ConfFileRWUtils.writeFile(PROPERTIES_SEQUENCE_DB_CONF + PROPERTIES_SUFFIX, sequenceConf);
            }
        }
    }

    @Override
    public void notifyCluster() throws Exception {
        JsonObject jsonObject = new JsonObject();
        String sequenceConf = ConfFileRWUtils.readFileWithOutError(PROPERTIES_SEQUENCE_CONF + PROPERTIES_SUFFIX);
        if (!"".equals(sequenceConf)) {
            jsonObject.addProperty(PROPERTIES_SEQUENCE_CONF, sequenceConf);
        }
        String sequenceDbConf = ConfFileRWUtils.readFileWithOutError(PROPERTIES_SEQUENCE_DB_CONF + PROPERTIES_SUFFIX);
        if (!"".equals(sequenceDbConf)) {
            jsonObject.addProperty(PROPERTIES_SEQUENCE_DB_CONF, sequenceDbConf);
        }
        ClusterHelper.setKV(CONFIG_PATH, (new Gson()).toJson(jsonObject));
    }

}
