/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml;

import com.actiontech.dble.DbleStartup;
import com.actiontech.dble.cluster.general.response.PauseShardingNodeResponse;
import com.actiontech.dble.cluster.values.OnlineType;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.xmltozk.XmltoZkMain;
import com.actiontech.dble.cluster.zkprocess.zktoxml.listen.*;
import com.actiontech.dble.config.model.ClusterConfig;
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
    public static PauseShardingNodeResponse getPauseShardingNodeListener() {
        return pauseShardingNodeListener;
    }

    private static PauseShardingNodeResponse pauseShardingNodeListener;
    private static OfflineStatusListener offlineStatusListener;

    private ZktoXmlMain() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ZktoXmlMain.class);

    public static void loadZkToFile() throws Exception {
        XmltoZkMain.initFileToZK();
        // load zk listen
        initListenerFromZK();
    }

    public static void loadZkListen() throws Exception {
        // load zk listen
        initListenerFromZK();
    }

    private static void initListenerFromZK() throws Exception {
        LOGGER.info("initListenerFromZK start");
        ZookeeperProcessListen zkListen = new ZookeeperProcessListen();
        Set<NotifyService> childService = new HashSet<>();

        // load sharding
        childService.add(new ShardingZkToXmlListener(zkListen));

        // load db
        childService.add(new DbGroupsZKToXmlListener(zkListen));

        // load user
        childService.add(new UserZkToXmlListener(zkListen));
        // load sequence
        childService.add(new SequenceToPropertiesListener(zkListen));

        new ConfigStatusListener(childService).registerPrefixForZk();

        offlineStatusListener = new OfflineStatusListener();
        offlineStatusListener.registerPrefixForZk();

        new BinlogPauseStatusListener().registerPrefixForZk();

        pauseShardingNodeListener = new PauseShardingNodeResponse();
        pauseShardingNodeListener.registerPrefixForZk();


        // notify all
        zkListen.initAllNode();
        zkListen.clearInited();
    }

    public static boolean serverStartDuringInitZKData() throws Exception {
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
                                ClusterConfig.getInstance().setInitZkFirst(true);
                                DbleStartup.initClusterAndServerStart();
                                return true;
                            }
                            break;
                        } finally {
                            LOGGER.info("initZKIfNot finish");
                            ClusterConfig.getInstance().setInitZkFirst(false);
                            confLock.release();
                        }
                    }
                }
            } else {
                try {
                    LOGGER.info("initFileToZK start");
                    ClusterConfig.getInstance().setInitZkFirst(true);
                    DbleStartup.initClusterAndServerStart();
                    return true;
                } finally {
                    LOGGER.info("initFileToZK end");
                    ClusterConfig.getInstance().setInitZkFirst(false);

                    confLock.release();
                }
            }
        }
        return false;
    }

    public static Map<String, OnlineType> getOnlineMap() {
        return offlineStatusListener.copyOnlineMap();
    }
}
