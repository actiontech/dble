/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml;

import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.xmltozk.XmltoZkMain;
import com.actiontech.dble.config.loader.zkprocess.zktoxml.listen.*;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * ZktoXml
 * author:liujun
 * Created:2016/9/20
 */
public final class ZktoXmlMain {
    private ZktoXmlMain() {
    }


    private static final Logger LOGGER = LoggerFactory.getLogger(ZktoXmlMain.class);

    /**
     * @throws Exception
     * @Created 2016/9/21
     */
    public static void loadZktoFile() throws Exception {
        // get zk conn
        CuratorFramework zkConn = ZKUtils.getConnection();
        //if first start,init zk
        initZKIfNot(zkConn);
        // load zk listen
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();
        initLocalConfFromZK(zkListen, zkConn);
        // load watch
        loadZkWatch(zkListen.getWatchPath(), zkConn, zkListen);

    }

    private static void initLocalConfFromZK(ZookeeperProcessListen zkListen, CuratorFramework zkConn) throws Exception {

        ConfigStatusListener confListener = new ConfigStatusListener(zkListen, zkConn);
        XmlProcessBase xmlProcess = new XmlProcessBase();

        // load schema
        new SchemaszkToxmlLoader(zkListen, zkConn, xmlProcess, confListener);

        // load server
        new ServerzkToxmlLoader(zkListen, zkConn, xmlProcess, confListener);

        // load rule
        new RuleszkToxmlLoader(zkListen, zkConn, xmlProcess, confListener);

        // load sequence
        new SequenceTopropertiesLoader(zkListen, zkConn);

        // load ehcache
        new EcacheszkToxmlLoader(zkListen, zkConn, xmlProcess);

        // transform bindata data to local file
        ZKUtils.addChildPathCache(KVPathUtil.getBinDataPath(), new BinDataPathChildrenCacheListener());

        new BinlogPauseStatusListener(zkListen, zkConn);

        // init xml
        xmlProcess.initJaxbClass();

        // notify all
        zkListen.initAllNode();
        zkListen.clearInited();
    }

    private static void initZKIfNot(CuratorFramework zkConn) throws Exception {
        String confInited = KVPathUtil.getConfInitedPath();
        //init conf if not
        if (zkConn.checkExists().forPath(confInited) == null) {
            InterProcessMutex confLock = new InterProcessMutex(zkConn, KVPathUtil.getConfInitLockPath());
            //someone acquired the lock
            if (!confLock.acquire(100, TimeUnit.MILLISECONDS)) {
                //loop wait for initialized
                while (true) {
                    if (!confLock.acquire(100, TimeUnit.MILLISECONDS)) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                    } else {
                        try {
                            if (zkConn.checkExists().forPath(confInited) == null) {
                                XmltoZkMain.initFileToZK();
                            }
                            break;
                        } finally {
                            confLock.release();
                        }
                    }
                }
            } else {
                try {
                    XmltoZkMain.initFileToZK();
                } finally {
                    confLock.release();
                }
            }
        }
    }

    private static void loadZkWatch(Set<String> setPaths, final CuratorFramework zkConn,
                                    final ZookeeperProcessListen zkListen) throws Exception {
        if (null != setPaths && !setPaths.isEmpty()) {
            for (String path : setPaths) {
                final NodeCache node = new NodeCache(zkConn, path);
                node.start(true);
                runWatch(node, zkListen);
                LOGGER.info("ZktoxmlMain loadZkWatch path:" + path + " regist success");
            }
        }
    }

    /**
     * @param cache    NodeCache
     * @param zkListen
     * @throws Exception
     * @Created 2016/9/20
     */
    private static void runWatch(final NodeCache cache, final ZookeeperProcessListen zkListen)
            throws Exception {
        cache.getListenable().addListener(new NodeCacheListener() {

            @Override
            public void nodeChanged() {
                LOGGER.info("ZktoxmlMain runWatch  process path  event start ");
                String notPath = cache.getCurrentData().getPath();
                LOGGER.info("NodeCache changed, path is: " + notPath);
                // notify
                zkListen.notify(notPath);
                LOGGER.info("ZktoxmlMain runWatch  process path  event over");
            }
        });
    }
}
