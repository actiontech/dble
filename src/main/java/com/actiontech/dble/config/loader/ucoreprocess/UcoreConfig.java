package com.actiontech.dble.config.loader.ucoreprocess;

import com.actiontech.dble.config.loader.ucoreprocess.KVtoXml.UcoreToXml;
import com.actiontech.dble.log.alarm.AlarmCode;
import com.actiontech.dble.util.ResourceUtil;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * Created by szf on 2018/1/24.
 */
public final class UcoreConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreConfig.class);
    private static UcoreConfig instance = new UcoreConfig();

    private static Properties ucoreProperties = null;

    private static final String UCORE_CONFIG_FILE_NAME = "/myid.properties";

    static {
        ucoreProperties = loadMyidPropersites();
    }

    private UcoreConfig() {

    }

    public static UcoreConfig getInstance() {
        return instance;
    }

    public String getValue(UcoreParamCfg param) {
        if (ucoreProperties != null && null != param) {
            return ucoreProperties.getProperty(param.getKey());
        }
        return null;
    }


    public void initUcore() {
        try {
            if (ucoreProperties != null && Boolean.parseBoolean(ucoreProperties.getProperty(UcoreParamCfg.UCORE_FLAGE.getKey()))) {
                UcoreToXml.loadKVtoFile();
            }
        } catch (Exception e) {
            LOGGER.error(AlarmCode.CORE_ZK_ERROR + "error:", e);
        }
    }


    private static Properties loadMyidPropersites() {
        Properties pros = new Properties();

        try (InputStream configIS = ResourceUtil.getResourceAsStream(UCORE_CONFIG_FILE_NAME)) {
            if (configIS == null) {
                return null;
            }
            pros.load(configIS);
        } catch (IOException e) {
            LOGGER.error(AlarmCode.CORE_ERROR + "ZkConfig LoadMyidPropersites error:", e);
            throw new RuntimeException("can't find myid properties file : " + UCORE_CONFIG_FILE_NAME);
        }

        // validate
        String zkURL = pros.getProperty(UcoreParamCfg.UCORE_CFG_URL.getKey());
        String myid = pros.getProperty(UcoreParamCfg.UCORE_CFG_MYID.getKey());

        String clusterId = pros.getProperty(UcoreParamCfg.UCORE_CFG_CLUSTERID.getKey());

        if (Strings.isNullOrEmpty(clusterId) || Strings.isNullOrEmpty(zkURL) || Strings.isNullOrEmpty(myid)) {
            throw new RuntimeException("clusterId and zkURL and myid must not be null or empty!");
        }
        return pros;

    }
}
