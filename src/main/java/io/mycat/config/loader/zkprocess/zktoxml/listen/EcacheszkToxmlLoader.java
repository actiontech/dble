package io.mycat.config.loader.zkprocess.zktoxml.listen;

import java.io.File;
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
import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;
import io.mycat.util.ResourceUtil;

/**
 * EcacheszkToxmlLoader
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class EcacheszkToxmlLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(EcacheszkToxmlLoader.class);

    private final String currZkPath;

    private final ParseXmlServiceInf<Ehcache> parseEcacheXMl;

    private ParseJsonServiceInf<Ehcache> parseJsonEhcacheService = new EhcacheJsonParse();

    private ZookeeperProcessListen zookeeperListen;

    public EcacheszkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                                XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        this.zookeeperListen = zookeeperListen;
        currZkPath = KVPathUtil.getCachePath();
        this.zookeeperListen.addToInit(this);
        parseEcacheXMl = new EhcacheParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        DiretoryInf rulesDirectory = new ZkDirectoryImpl(currZkPath, null);
        this.getTreeDirectory(currZkPath, KVPathUtil.CACHE, rulesDirectory);

        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) rulesDirectory.getSubordinateInfo().get(0);

        zktoEhcacheWrite(zkDirectory);

        LOGGER.info("EcacheszkToxmlLoader notifyProcess   zk ehcache write success ");

        return true;
    }

    /**
     * zktoEhcacheWrite
     *
     * @param zkDirectory
     * @return
     * @Created 2016/9/17
     */
    private void zktoEhcacheWrite(ZkDirectoryImpl zkDirectory) {

        DataInf ehcacheZkDirectory = this.getZkData(zkDirectory, KVPathUtil.EHCACHE_NAME);

        Ehcache ehcache = parseJsonEhcacheService.parseJsonToBean(ehcacheZkDirectory.getDataValue());

        String outputPath = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        outputPath = new File(outputPath).getPath() + File.separator;
        outputPath += KVPathUtil.EHCACHE_NAME;

        parseEcacheXMl.parseToXmlWrite(ehcache, outputPath, null);

        this.zookeeperListen.addWatch(KVPathUtil.getEhcacheNamePath(), this);

        DataInf cacheserZkDirectory = this.getZkData(zkDirectory, KVPathUtil.CACHESERVER_NAME);

        if (null != cacheserZkDirectory) {
            ZkDataImpl cacheData = (ZkDataImpl) cacheserZkDirectory;

            try {
                ConfFileRWUtils.writeFile(cacheData.getName(), cacheData.getValue());
            } catch (IOException e) {
                LOGGER.error("EcacheszkToxmlLoader wirteMapFile IOException", e);
            }
            this.zookeeperListen.addWatch(KVPathUtil.getCacheServerNamePath(), this);
        }

    }
}
