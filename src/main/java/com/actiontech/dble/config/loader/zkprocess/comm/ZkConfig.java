/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.comm;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.zkprocess.zktoxml.ZktoXmlMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private ZkConfig() {
    }

    private static ZkConfig zkCfgInstance = new ZkConfig();

    private static Properties zkProperties = null;


    public String getZkURL() {
        return zkProperties == null ? null :
                zkProperties.getProperty(ClusterParamCfg.CLUSTER_PLUGINS_IP.getKey());
    }

    public static void initZk(Properties cluterProperties) {
        try {
            zkProperties = cluterProperties;
            ZktoXmlMain.loadZktoFile();
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
    public String getValue(ClusterParamCfg param) {
        if (zkProperties != null && null != param) {
            return zkProperties.getProperty(param.getKey());
        }
        return null;
    }

    public static void setZkProperties(Properties zkProperties) {
        ZkConfig.zkProperties = zkProperties;
    }

}
