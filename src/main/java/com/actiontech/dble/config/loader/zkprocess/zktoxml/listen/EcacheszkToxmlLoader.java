/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.config.loader.console.ZookeeperPath;
import com.actiontech.dble.config.loader.zkprocess.comm.ConfFileRWUtils;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.entity.cache.Ehcache;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.cache.json.EhcacheJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.cache.xml.EhcacheParseXmlImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DataInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.DiretoryInf;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ResourceUtil;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * EcacheszkToxmlLoader
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
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
