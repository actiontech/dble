/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.logic;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.FeedBackType;
import com.oceanbase.obsharding_d.cluster.values.PauseInfo;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import com.oceanbase.obsharding_d.singleton.PauseShardingNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public class PauseResumeClusterLogic extends AbstractClusterLogic {
    private static final Logger LOGGER = LoggerFactory.getLogger(PauseResumeClusterLogic.class);

    PauseResumeClusterLogic() {
        super(ClusterOperation.PAUSE_RESUME);
    }

    public Thread pauseShardingNodeEvent(PauseInfo pauseInfo, final Lock lock) throws Exception {

        if (pauseInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            return null;
        }
        final String shardingNodes = pauseInfo.getShardingNodes();
        Thread waitThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Start to pause shardingNode " + shardingNodes);
                    Set<String> shardingNodeSet = new HashSet<>(Arrays.asList(shardingNodes.split(",")));
                    PauseShardingNodeManager.getInstance().startPausing(pauseInfo.getConnectionTimeOut(), shardingNodeSet, shardingNodes, pauseInfo.getQueueLimit());

                    while (!Thread.currentThread().isInterrupted()) {
                        lock.lock();
                        try {
                            boolean nextTurn = false;
                            for (IOProcessor processor : OBsharding_DServer.getInstance().getFrontProcessors()) {
                                for (Map.Entry<Long, FrontendConnection> entry : processor.getFrontends().entrySet()) {
                                    AbstractService service = entry.getValue().getService();
                                    if (service instanceof ShardingService) {
                                        ShardingService shardingService = (ShardingService) service;
                                        for (Map.Entry<RouteResultsetNode, BackendConnection> conEntry : shardingService.getSession2().getTargetMap().entrySet()) {
                                            if (shardingNodeSet.contains(conEntry.getKey().getName())) {
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
                                LOGGER.info("create self pause node.");
                                clusterHelper.createSelfTempNode(ClusterPathUtil.getPauseResultNodePath(), FeedBackType.SUCCESS);
                                break;
                            }
                        } finally {
                            lock.unlock();
                        }
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100L));
                    }
                    LOGGER.info("pause for slave done. interruptedFlag:" + Thread.currentThread().isInterrupted());

                } catch (Exception e) {
                    LOGGER.warn("the ucore pause error", e);
                }

            }
        });
        waitThread.start();
        return waitThread;

    }

    public void resumeShardingNodeEvent(PauseInfo pauseInfo, final Lock lock, Thread waitThread) throws Exception {
        if (pauseInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            return;
        }

        lock.lock();
        try {
            if (waitThread != null && waitThread.isAlive()) {
                waitThread.interrupt();
            }
        } finally {
            lock.unlock();
        }
        LOGGER.info("resume shardingNode for get notice");
        PauseShardingNodeManager.getInstance().resume();
        clusterHelper.createSelfTempNode(ClusterPathUtil.getPauseResumePath(), FeedBackType.ofSuccess(SystemConfig.getInstance().getInstanceName()));
    }
}
