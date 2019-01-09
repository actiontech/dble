/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreXmlLoader;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.PauseInfo;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean.DELETE;


public class UPauseDataNodeResponse implements UcoreXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(UPauseDataNodeResponse.class);

    private static final String CONFIG_PATH = UcorePathUtil.getPauseDataNodePath();

    private Thread waitThread;

    private final Lock lock = new ReentrantLock();

    public UPauseDataNodeResponse(UcoreClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
        confListener.addChild(this, UcorePathUtil.getPauseResumePath());
    }

    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {
        LOGGER.info("get key in UPauseDataNodeResponse:" + configValue.getKey() + "   " + configValue.getValue());
        if (!DELETE.equals(configValue.getChangeType())) {
            if (configValue.getKey().equals(UcorePathUtil.getPauseDataNodePath()) || UcorePathUtil.getPauseResumePath().equals(configValue.getKey())) {
                final PauseInfo pauseInfo = new PauseInfo(configValue.getValue());
                if (!pauseInfo.getFrom().equals(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
                    if (PauseInfo.PAUSE.equals(pauseInfo.getType())) {
                        final String dataNodes = pauseInfo.getDataNodes();
                        waitThread = new Thread(new Runnable() {
                            @Override
                            public void run() {

                                try {
                                    LOGGER.info("Strat pause dataNode " + dataNodes);
                                    Set<String> dataNodeSet = new HashSet<>(Arrays.asList(dataNodes.split(",")));
                                    DbleServer.getInstance().getMiManager().startPausing(pauseInfo.getConnectionTimeOut(), dataNodeSet, pauseInfo.getQueueLimit());

                                    while (!Thread.interrupted()) {
                                        lock.lock();
                                        try {
                                            boolean nextTurn = false;
                                            for (NIOProcessor processor : DbleServer.getInstance().getFrontProcessors()) {
                                                for (Map.Entry<Long, FrontendConnection> entry : processor.getFrontends().entrySet()) {
                                                    if (entry.getValue() instanceof ServerConnection) {
                                                        ServerConnection sconnection = (ServerConnection) entry.getValue();
                                                        for (Map.Entry<RouteResultsetNode, BackendConnection> conEntry : sconnection.getSession2().getTargetMap().entrySet()) {
                                                            if (dataNodeSet.contains(conEntry.getKey().getName())) {
                                                                nextTurn = true;
                                                                break;
                                                            }
                                                        }
                                                        if (nextTurn) {
                                                            break;
                                                        }
                                                    }
                                                }
                                                if (nextTurn) {
                                                    break;
                                                }
                                            }
                                            if (!nextTurn) {
                                                ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getPauseResultNodePath(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID)),
                                                        UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
                                                break;
                                            }
                                        } finally {
                                            lock.unlock();
                                        }
                                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100L));
                                    }

                                } catch (Exception e) {
                                    LOGGER.warn(" the ucore pause error " + e.getMessage());
                                }

                            }
                        });
                        waitThread.start();
                    } else {
                        lock.lock();
                        try {
                            if (waitThread.isAlive()) {
                                waitThread.interrupt();
                            }
                        } finally {
                            lock.unlock();
                        }
                        LOGGER.info("resume dataNodes for get notice");
                        DbleServer.getInstance().getMiManager().resume();
                        ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getPauseResumePath(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID)),
                                UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));

                    }
                }
            }
        }
    }

    /**
     * notify the cluster that the pause is over
     */
    @Override
    public void notifyCluster() throws Exception {
        lock.lock();
        try {
            if (waitThread.isAlive()) {
                waitThread.interrupt();
            }
        } finally {
            lock.unlock();
        }
        DbleServer.getInstance().getMiManager().resume();
    }
}
