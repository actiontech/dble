package io.mycat.config.loader.zkprocess.xmltozk.listen;


import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * EcachesxmlTozkLoader
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class EcachesxmlTozkLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(EcachesxmlTozkLoader.class);

    private final String currZkPath;


    private final ParseXmlServiceInf<Ehcache> parseEcacheXMl;

    private ParseJsonServiceInf<Ehcache> parseJsonEhcacheService = new EhcacheJsonParse();

    public EcachesxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getCachePath();
        zookeeperListen.addToInit(this);

        parseEcacheXMl = new EhcacheParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        Ehcache ehcache = this.parseEcacheXMl.parseXmlToBean(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey() + KVPathUtil.EHCACHE_NAME);
        LOGGER.info("EhcachexmlTozkLoader notifyProcess xml to zk Ehcache Object  :" + ehcache);
        this.xmlTozkEhcacheJson(currZkPath, ehcache);

        LOGGER.info("EhcachexmlTozkLoader notifyProcess xml to zk is success");

        return true;
    }

    /**
     * xmlTozkEhcacheJson
     *
     * @param basePath
     * @param ehcache
     * @throws Exception
     * @Created 2016/9/17
     */
    private void xmlTozkEhcacheJson(String basePath, Ehcache ehcache) throws Exception {
        String ehcacheJson = this.parseJsonEhcacheService.parseBeanToJson(ehcache);
        this.checkAndwriteString(basePath, KVPathUtil.EHCACHE_NAME, ehcacheJson);

        try {
            String serviceValue = ConfFileRWUtils.readFile(KVPathUtil.CACHESERVER_NAME);
            this.checkAndwriteString(basePath, KVPathUtil.CACHESERVER_NAME, serviceValue);
        } catch (IOException e) {
            LOGGER.error("EhcachexmlTozkLoader readMapFile IOException", e);
        }
    }
}
