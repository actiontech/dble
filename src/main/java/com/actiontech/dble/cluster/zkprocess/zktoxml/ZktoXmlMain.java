/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.cluster.zkprocess.xmltozk.XmltoZkMain;
import com.actiontech.dble.cluster.zkprocess.zktoxml.listen.*;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * ZktoXml
 * author:liujun
 * Created:2016/9/20
 */
public final class ZktoXmlMain {
    public static PauseShardingNodeListener getPauseShardingNodeListener() {
        return pauseShardingNodeListener;
    }

    private static PauseShardingNodeListener pauseShardingNodeListener;
    private static OfflineStatusListener offlineStatusListener;
    private ZktoXmlMain() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ZktoXmlMain.class);

    public static void loadZkToFile() throws Exception {

        //if first start,init zk
        initZKIfNot();
        // load zk listen

        initListenerFromZK();
    }

    private static void initListenerFromZK() throws Exception {
        LOGGER.info("initListenerFromZK start");
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();
        Set<NotifyService> childService = new HashSet<>();
        XmlProcessBase xmlProcess = new XmlProcessBase();

        // load sharding
        childService.add(new ShardingZkToXmlListener(zkListen, xmlProcess));

        // load db
        childService.add(new DbGroupsZKToXmlListener(zkListen, xmlProcess));

        // load user
        childService.add(new UserZkToXmlListener(zkListen, xmlProcess));
        // load sequence
        childService.add(new SequenceToPropertiesListener(zkListen));

        ZKUtils.addChildPathCache(ClusterPathUtil.getConfStatusPath(), new ConfigStatusListener(childService));

        offlineStatusListener = new OfflineStatusListener();
        ZKUtils.addChildPathCache(ClusterPathUtil.getOnlinePath(), offlineStatusListener);

        ZKUtils.addChildPathCache(ClusterPathUtil.getBinlogPause(), new BinlogPauseStatusListener());

        pauseShardingNodeListener = new PauseShardingNodeListener();
        ZKUtils.addChildPathCache(ClusterPathUtil.getPauseShardingNodePath(), pauseShardingNodeListener);


        // init xml
        xmlProcess.initJaxbClass();

        // notify all
        zkListen.initAllNode();
        zkListen.clearInited();
    }

    private static void initZKIfNot() throws Exception {
        // get zk conn
        CuratorFramework zkConn = ZKUtils.getConnection();
        String confInited = KVPathUtil.getConfInitedPath();
        //init conf if not
        if (zkConn.checkExists().forPath(confInited) == null) {
            InterProcessMutex confLock = new InterProcessMutex(zkConn, KVPathUtil.getConfInitLockPath());
            //someone acquired the lock
            if (!confLock.acquire(100, TimeUnit.MILLISECONDS)) {
                LOGGER.info("acquire lock failed");
                //loop wait for initialized
                while (true) {
                    if (!confLock.acquire(100, TimeUnit.MILLISECONDS)) {
                        LOGGER.info("acquire lock failed");
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                    } else {
                        LOGGER.info("acquire lock success");
                        try {
                            if (zkConn.checkExists().forPath(confInited) == null) {
                                LOGGER.info("initFileToZK start");
                                XmltoZkMain.initFileToZK();
                                LOGGER.info("initFileToZK end");
                            }
                            break;
                        } finally {
                            LOGGER.info("initZKIfNot finish");
                            confLock.release();
                        }
                    }
                }
            } else {
                try {
                    LOGGER.info("initFileToZK start");
                    XmltoZkMain.initFileToZK();
                } finally {
                    LOGGER.info("initFileToZK end");
                    confLock.release();
                }
            }
        }
    }

    public static Map<String, String> getOnlineMap() {
        return offlineStatusListener.copyOnlineMap();
    }
}
