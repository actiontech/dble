/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.comm;

import com.actiontech.dble.config.loader.zkprocess.zktoxml.ZktoXmlMain;
import com.actiontech.dble.util.ResourceUtil;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * ZkConfig
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public final class ZkConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkConfig.class);

    private static final String ZK_CONFIG_FILE_NAME = "/myid.properties";

    private ZkConfig() {
    }

    private static ZkConfig zkCfgInstance = new ZkConfig();

    private static Properties zkProperties = null;

    static {
        zkProperties = loadMyidPropersites();
    }


    public String getZkURL() {
        return zkProperties == null ? null : zkProperties.getProperty(ZkParamCfg.ZK_CFG_URL.getKey());
    }

    public void initZk() {
        try {
            if (zkProperties != null && Boolean.parseBoolean(zkProperties.getProperty(ZkParamCfg.ZK_CFG_FLAG.getKey()))) {
                ZktoXmlMain.loadZktoFile();
            }
        } catch (Exception e) {
            LOGGER.error("error:", e);
        }
    }

    /**
     * @return
     * @Created 2016/9/15
     */
    public static ZkConfig getInstance() {

        return zkCfgInstance;
    }

    /**
     * get property from myid
     *
     * @param param
     * @return
     * @Created 2016/9/15
     */
    public String getValue(ZkParamCfg param) {
        if (zkProperties != null && null != param) {
            return zkProperties.getProperty(param.getKey());
        }

        return null;
    }

    /**
     * @return
     * @Created 2016/9/15
     */
    private static Properties loadMyidPropersites() {
        Properties pros = new Properties();

        try (InputStream configIS = ResourceUtil.getResourceAsStream(ZK_CONFIG_FILE_NAME)) {
            if (configIS == null) {
                return null;
            }

            pros.load(configIS);
        } catch (IOException e) {
            LOGGER.error("ZkConfig LoadMyidPropersites error:", e);
            throw new RuntimeException("can't find myid properties file : " + ZK_CONFIG_FILE_NAME);
        }

        // validate
        String zkURL = pros.getProperty(ZkParamCfg.ZK_CFG_URL.getKey());
        String myid = pros.getProperty(ZkParamCfg.ZK_CFG_MYID.getKey());

        String clusterId = pros.getProperty(ZkParamCfg.ZK_CFG_CLUSTERID.getKey());

        if (Strings.isNullOrEmpty(clusterId) || Strings.isNullOrEmpty(zkURL) || Strings.isNullOrEmpty(myid)) {
            throw new RuntimeException("clusterId and zkURL and myid must not be null or empty!");
        }
        return pros;

    }

}
