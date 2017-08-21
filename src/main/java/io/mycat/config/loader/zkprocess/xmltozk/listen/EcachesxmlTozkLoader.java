package io.mycat.config.loader.zkprocess.xmltozk.listen;


import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ConfFileRWUtils;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.entity.cache.Ehcache;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.cache.json.EhcacheJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.cache.xml.EhcacheParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 进行从ecache.xml加载到zk中加载
 * 源文件名：SchemasLoader.java
 * 文件版本：1.0.0
 * 创建作者：liujun
 * 创建日期：2016年9月15日
 * 修改作者：liujun
 * 修改日期：2016年9月15日
 * 文件描述：TODO
 * 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
 */
public class EcachesxmlTozkLoader extends ZkMultLoader implements NotifyService {

    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(EcachesxmlTozkLoader.class);

    /**
     * 当前文件中的zkpath信息
     */
    private final String currZkPath;


    /**
     * ehcache的xml的转换信息
     */
    private final ParseXmlServiceInf<Ehcache> parseEcacheXMl;

    /**
     * 表的路由信息
     */
    private ParseJsonServiceInf<Ehcache> parseJsonEhcacheService = new EhcacheJsonParse();

    public EcachesxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getCachePath();
        // 将当前自己注册为事件接收对象
        zookeeperListen.addToInit(this);

        // 生成xml与类的转换信息
        parseEcacheXMl = new EhcacheParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        // 1,读取本地的xml文件
        Ehcache Ehcache = this.parseEcacheXMl.parseXmlToBean(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey() + KVPathUtil.EHCACHE_NAME);
        LOGGER.info("EhcachexmlTozkLoader notifyProcess xml to zk Ehcache Object  :" + Ehcache);
        // 将实体信息写入至zk中
        this.xmlTozkEhcacheJson(currZkPath, Ehcache);

        LOGGER.info("EhcachexmlTozkLoader notifyProcess xml to zk is success");

        return true;
    }

    /**
     * 将xml文件的信息写入到zk中
     * 方法描述
     *
     * @param basePath 基本路径
     * @param ehcache  文件的信息
     * @throws Exception 异常信息
     * @创建日期 2016年9月17日
     */
    private void xmlTozkEhcacheJson(String basePath, Ehcache ehcache) throws Exception {
        // ehcache节点信息
        String ehcacheJson = this.parseJsonEhcacheService.parseBeanToJson(ehcache);
        this.checkAndwriteString(basePath, KVPathUtil.EHCACHE_NAME, ehcacheJson);

        // 读取文件信息
        try {
            String serviceValue = ConfFileRWUtils.readFile(KVPathUtil.CACHESERVER_NAME);
            this.checkAndwriteString(basePath, KVPathUtil.CACHESERVER_NAME, serviceValue);
        } catch (IOException e) {
            LOGGER.error("EhcachexmlTozkLoader readMapFile IOException", e);
        }
    }
}
