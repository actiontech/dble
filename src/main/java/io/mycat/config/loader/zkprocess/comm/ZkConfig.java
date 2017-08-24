package io.mycat.config.loader.zkprocess.comm;

import com.google.common.base.Strings;
import io.mycat.config.loader.zkprocess.zktoxml.ZktoXmlMain;
import io.mycat.util.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * 进行zk的配制信息
 * 源文件名：ZkConfig.java
 * 文件版本：1.0.0
 * 创建作者：liujun
 * 创建日期：2016年9月15日
 * 修改作者：liujun
 * 修改日期：2016年9月15日
 * 文件描述：TODO
 * 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
 */
public final class ZkConfig {
    /**
     * 日志信息
     *
     * @字段说明 LOGGER
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkConfig.class);

    private static final String ZK_CONFIG_FILE_NAME = "/myid.properties";

    private ZkConfig() {
    }

    /**
     * 实例对象信息
     *
     * @字段说明 ZKCFGINSTANCE
     */
    private static ZkConfig zkCfgInstance = new ZkConfig();


    /**
     * myid的属性文件信息
     *
     * @字段说明 ZKPROPERTIES
     */
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
     * 获得实例对象信息
     * 方法描述
     *
     * @return
     * @创建日期 2016年9月15日
     */
    public static ZkConfig getInstance() {

        return zkCfgInstance;
    }

    /**
     * 获取myid属性文件中的属性值
     * 方法描述
     *
     * @param param 参数信息
     * @return
     * @创建日期 2016年9月15日
     */
    public String getValue(ZkParamCfg param) {
        if (zkProperties != null && null != param) {
            return zkProperties.getProperty(param.getKey());
        }

        return null;
    }

    /**
     * 加载myid配制文件信息
     * 方法描述
     *
     * @return
     * @创建日期 2016年9月15日
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
