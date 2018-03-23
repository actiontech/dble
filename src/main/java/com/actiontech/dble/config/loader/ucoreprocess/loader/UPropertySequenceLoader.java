package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.comm.ConfFileRWUtils;
import com.alibaba.fastjson.JSONObject;


/**
 * Created by szf on 2018/1/29.
 */
public class UPropertySequenceLoader implements UcoreXmlLoader {

    private static final String PROPERTIES_SUFFIX = ".properties";

    private static final String PROPERTIES_SEQUENCE_CONF = "sequence_conf";

    private static final String PROPERTIES_SEQUENCE_DB_CONF = "sequence_db_conf";

    private static final String PROPERTIES_CACHESERVER_NAME = "cacheservice";

    private static final String PROPERTIES_SEQUENCE_DISTRIBUTED_CONF = "sequence_distributed_conf";


    private static final String CONFIG_PATH = UcorePathUtil.getSequencesPath();


    public UPropertySequenceLoader(UcoreClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {

        UKvBean lock = ClusterUcoreSender.getKey(UcorePathUtil.getConfChangeLockPath());
        if (UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID).equals(lock.getValue())) {
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

            if (jsonObj.get(PROPERTIES_SEQUENCE_DISTRIBUTED_CONF) != null) {
                String sequenceConf = jsonObj.getString(PROPERTIES_SEQUENCE_DISTRIBUTED_CONF);
                ConfFileRWUtils.writeFile(PROPERTIES_SEQUENCE_DISTRIBUTED_CONF + PROPERTIES_SUFFIX, sequenceConf);
            }

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
        String sequenceConf = ConfFileRWUtils.readFileWithOutError(PROPERTIES_SEQUENCE_CONF + PROPERTIES_SUFFIX);
        if (sequenceConf != null && !"".equals(sequenceConf)) {
            jsonObject.put(PROPERTIES_SEQUENCE_CONF, sequenceConf);
        }
        String sequenceDbConf = ConfFileRWUtils.readFileWithOutError(PROPERTIES_SEQUENCE_DB_CONF + PROPERTIES_SUFFIX);
        if (sequenceDbConf != null && !"".equals(sequenceDbConf)) {
            jsonObject.put(PROPERTIES_SEQUENCE_DB_CONF, sequenceDbConf);
        }
        String sequenceDistributedConf = ConfFileRWUtils.readFileWithOutError(PROPERTIES_SEQUENCE_DISTRIBUTED_CONF + PROPERTIES_SUFFIX);
        if (sequenceDistributedConf != null && !"".equals(sequenceDistributedConf)) {
            jsonObject.put(PROPERTIES_SEQUENCE_DISTRIBUTED_CONF, sequenceDistributedConf);
        }
        ClusterUcoreSender.sendDataToUcore(CONFIG_PATH, jsonObject.toJSONString());
    }

}
