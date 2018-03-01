package com.actiontech.dble.config.loader.ucoreprocess;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.KVtoXml.UcoreToXml;
import com.actiontech.dble.log.alarm.AlarmCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * Created by szf on 2018/1/24.
 */
public final class UcoreConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreConfig.class);
    private static UcoreConfig instance = new UcoreConfig();

    private static Properties ucoreProperties = null;

    private UcoreConfig() {

    }

    public static UcoreConfig getInstance() {
        return instance;
    }

    public String getValue(ClusterParamCfg param) {
        if (ucoreProperties != null && null != param) {
            return ucoreProperties.getProperty(param.getKey());
        }
        return null;
    }


    /**
     * init the ucore and set keys
     * @param cluterProperties
     */
    public static void initUcore(Properties cluterProperties) {
        try {
            ucoreProperties = cluterProperties;
            UcoreToXml.loadKVtoFile();
        } catch (Exception e) {
            LOGGER.error(AlarmCode.CORE_ZK_ERROR + "error:", e);
        }
    }

}
