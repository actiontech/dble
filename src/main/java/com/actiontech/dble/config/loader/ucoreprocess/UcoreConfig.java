/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.KVtoXml.UcoreToXml;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreNodesListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.actiontech.dble.cluster.ClusterController.CONFIG_FILE_NAME;


/**
 * Created by szf on 2018/1/24.
 */
public final class UcoreConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreConfig.class);
    private static UcoreConfig instance = new UcoreConfig();


    private List<String> ipList = new ArrayList<>();

    private Properties ucoreProperties = null;

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
     *
     * @param cluterProperties
     */
    public static void initUcore(Properties cluterProperties) {
        try {
            getInstance().ucoreProperties = cluterProperties;
            for (String ip : cluterProperties.getProperty(ClusterParamCfg.CLUSTER_PLUGINS_IP.getKey()).split(",")) {
                getInstance().ipList.add(ip);
            }
            UcoreToXml.loadKVtoFile();
        } catch (Exception e) {
            LOGGER.error("error:", e);
        }
    }


    /**
     * init the ucore and set keys
     *
     * @param cluterProperties
     */
    public static void initUcoreFromShell(Properties cluterProperties) {
        try {
            getInstance().ucoreProperties = cluterProperties;
            for (String ip : cluterProperties.getProperty(ClusterParamCfg.CLUSTER_PLUGINS_IP.getKey()).split(",")) {
                getInstance().ipList.add(ip);
            }
        } catch (Exception e) {
            LOGGER.error("error:", e);
        }
    }

    public static void setUcoreProperties(Properties ucoreProperties) {
        getInstance().ucoreProperties = ucoreProperties;
    }

    public void setIpList(List<String> ipList) {
        this.ipList = ipList;
    }

    public void setIp(String ips) {
        getInstance().ucoreProperties.setProperty(ClusterParamCfg.CLUSTER_PLUGINS_IP.getKey(), ips);
        FileOutputStream out = null;
        try {
            File file = new File(UcoreNodesListener.class.getResource(CONFIG_FILE_NAME).getFile());
            out = new FileOutputStream(file);
            getInstance().ucoreProperties.store(out, "");
        } catch (Exception e) {
            LOGGER.info("ips set to ucore failure");
        } finally {
            try {
                out.close();
            } catch (Exception e) {
                LOGGER.info("open file error");
            }
        }
    }

    public List<String> getIpList() {
        return ipList;
    }


}
