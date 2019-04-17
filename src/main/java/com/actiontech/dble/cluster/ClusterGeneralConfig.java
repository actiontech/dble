package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.impl.UcoreSender;
import com.actiontech.dble.cluster.impl.ushard.UshardSender;
import com.actiontech.dble.cluster.kVtoXml.ClusterToXml;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;

import java.util.Properties;

import static com.actiontech.dble.backend.mysql.nio.handler.ResetConnHandler.LOGGER;
import static com.actiontech.dble.cluster.ClusterController.*;

/**
 * Created by szf on 2019/3/11.
 */
public final class ClusterGeneralConfig {

    private static final ClusterGeneralConfig INSTANCE = new ClusterGeneralConfig();
    private boolean useCluster = false;
    private AbstractClusterSender clusterSender = null;
    private Properties properties = null;
    private String clusterType = null;

    private ClusterGeneralConfig() {

    }

    public static ClusterGeneralConfig initConfig(Properties properties) {
        INSTANCE.properties = properties;

        if (CONFIG_MODE_USHARD.equalsIgnoreCase(properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()))) {
            INSTANCE.clusterSender = new UshardSender();
            INSTANCE.useCluster = true;
            INSTANCE.clusterType = CONFIG_MODE_USHARD;
        } else if (CONFIG_MODE_UCORE.equalsIgnoreCase(properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()))) {
            INSTANCE.clusterSender = new UcoreSender();
            INSTANCE.useCluster = true;
            INSTANCE.clusterType = CONFIG_MODE_UCORE;
        } else if (CONFIG_MODE_ZK.equalsIgnoreCase(properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()))) {
            INSTANCE.useCluster = true;
            INSTANCE.clusterType = CONFIG_MODE_ZK;
        } else if (CONFIG_MODE_SINGLE.equalsIgnoreCase(properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()))) {
            LOGGER.info("No Cluster Config .......start in single mode");
        } else {
            try {
                String clazz = properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey());
                INSTANCE.useCluster = true;
                INSTANCE.clusterType = CONFIG_MODE_CUSTOMIZATION;
                Class<?> clz = Class.forName(clazz);
                //all function must be extend from AbstractPartitionAlgorithm
                if (!AbstractClusterSender.class.isAssignableFrom(clz)) {
                    throw new IllegalArgumentException("No ClusterSender AS " + clazz);
                }
                INSTANCE.clusterSender = (AbstractClusterSender) clz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Get error when try to create " + properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()));
            }
        }
        return INSTANCE;
    }


    public static void initData(Properties properties) {
        if (CONFIG_MODE_ZK.equalsIgnoreCase(properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()))) {
            ZkConfig.initZk(properties);
        } else if (CONFIG_MODE_SINGLE.equalsIgnoreCase(properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()))) {
            LOGGER.info("No Cluster Config .......start in single mode");
        } else {
            try {
                INSTANCE.clusterSender.checkClusterConfig(properties);
                INSTANCE.clusterSender.initCluster(properties);
                ClusterToXml.loadKVtoFile();
            } catch (Exception e) {
                throw new RuntimeException("Get error when try to create " + properties.getProperty(ClusterParamCfg.CLUSTER_FLAG.getKey()));
            }
        }
    }


    public AbstractClusterSender getClusterSender() {
        return clusterSender;
    }

    public void setClusterSender(AbstractClusterSender clusterSender) {
        this.clusterSender = clusterSender;
    }

    public boolean isUseCluster() {
        return useCluster;
    }

    public String getClusterType() {
        return clusterType;
    }

    public static ClusterGeneralConfig getInstance() {
        return INSTANCE;
    }

    public String getValue(ClusterParamCfg param) {
        if (properties != null && null != param) {
            return properties.getProperty(param.getKey());
        }
        return null;
    }

}
