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
import io.mycat.util.ResourceUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkNotNull;

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
     * Ehcache文件的路径信息
    */
    private static final String EHCACHE_PATH = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + "ehcache.xml";

    /**
     * 缓存文件名称
    */
    private static final String CACHESERVER_NAME = "cacheservice.properties";

    /**
     * 缓存的xml文件配制信息
    */
    private static final String EHCACHE_NAME = "ehcache.xml";

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

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath() + ZookeeperPath.FLOW_ZK_PATH_CACHE.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

        // 生成xml与类的转换信息
        parseEcacheXMl = new EhcacheParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess(boolean isAll) throws Exception {
        // 1,读取本地的xml文件
        Ehcache Ehcache = this.parseEcacheXMl.parseXmlToBean(EHCACHE_PATH);
        LOGGER.info("EhcachexmlTozkLoader notifyProcess xml to zk Ehcache Object  :" + Ehcache);
        // 将实体信息写入至zk中
        this.xmlTozkEhcacheJson(currZkPath, Ehcache);

        LOGGER.info("EhcachexmlTozkLoader notifyProcess xml to zk is success");

        return true;
    }

    /**
     * 将xml文件的信息写入到zk中
    * 方法描述
    * @param basePath 基本路径
    * @param ehcache 文件的信息
    * @throws Exception 异常信息
    * @创建日期 2016年9月17日
    */
    private void xmlTozkEhcacheJson(String basePath, Ehcache ehcache) throws Exception {
        // ehcache节点信息
        String ehcacheFile = ZookeeperPath.ZK_SEPARATOR.getKey() + EHCACHE_NAME;
        String ehcacheJson = this.parseJsonEhcacheService.parseBeanToJson(ehcache);
        this.checkAndwriteString(basePath, ehcacheFile, ehcacheJson);

        // 读取文件信息
        String cacheServicePath = ZookeeperPath.ZK_SEPARATOR.getKey() + CACHESERVER_NAME;
        try {
            String serviceValue = ConfFileRWUtils.readFile(CACHESERVER_NAME);
            this.checkAndwriteString(basePath, cacheServicePath, serviceValue);
        }catch (IOException e) {
            LOGGER.error("EhcachexmlTozkLoader readMapFile IOException", e);
        }
    }
}
