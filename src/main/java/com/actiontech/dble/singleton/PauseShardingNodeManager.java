/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.DistributeLock;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.values.PauseInfo;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import com.actiontech.dble.meta.PauseEndThreadPool;
import com.actiontech.dble.meta.SchemaMeta;
import com.actiontech.dble.meta.TableMeta;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.actiontech.dble.cluster.values.PauseInfo.PAUSE;

public final class PauseShardingNodeManager {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PauseShardingNodeManager.class);
    private static final PauseShardingNodeManager INSTANCE = new PauseShardingNodeManager();
    private ReentrantLock pauseLock = new ReentrantLock();
    private volatile Set<String> shardingNodes = null;
    private Map<String, Set<String>> pauseMap = new ConcurrentHashMap<>();
    private AtomicBoolean isPausing = new AtomicBoolean(false);
    private volatile DistributeLock distributeLock = null;

    private volatile PauseEndThreadPool pauseThreadPool = null;

    private PauseShardingNodeManager() {

    }

    public static PauseShardingNodeManager getInstance() {
        return INSTANCE;
    }

    public AtomicBoolean getIsPausing() {
        return this.isPausing;
    }


    public void fetchClusterStatus() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            DistributeLock tempPauseLock = ClusterHelper.createDistributeLock(ClusterPathUtil.getPauseShardingNodeLockPath(),
                    SystemConfig.getInstance().getInstanceName());
            boolean locked = false;
            try {
                for (int i = 0; i < 5; i++) {
                    if (!tempPauseLock.acquire()) {
                        //cluster is doing pause or resume. normally ,it will release lock soon(unless a transaction hanged the operation).
                        //so, need try again later.
                        LOGGER.warn("another node is doing pause or resume. We will try to wait a while.");
                    } else {
                        locked = true;
                        break;
                    }
                    Thread.sleep(3000);
                }

                if (!locked) {
                    final String msg = "Other node in cluster is doing pause/resume operation. We can't bootstrap unless this operation is ok.";
                    LOGGER.error(msg);
                    throw new IllegalStateException(msg);
                }

                final KvBean pauseResultNode = ClusterHelper.getKV(ClusterPathUtil.getPauseResultNodePath());
                if (pauseResultNode != null) {
                    //cluster is Pausing
                    final PauseInfo pauseInfo = new PauseInfo(pauseResultNode.getValue());
                    Set<String> shardingNodeSet = new HashSet<>(Arrays.asList(pauseInfo.getShardingNodes().split(",")));

                    //pause self
                    if (!getInstance().startPausing(pauseInfo.getConnectionTimeOut(), shardingNodeSet, pauseInfo.getQueueLimit())) {
                        //the error message can only show in single mode. So, this situation won't happen.
                        final String msg = "IllegalState: shardingNodes is paused already";
                        LOGGER.error(msg);
                        throw new IllegalStateException(msg);
                    }
                }
                //cluster is resuming.
                //just return;
                return;
            } finally {
                if (locked) {
                    tempPauseLock.release();
                }

            }
        }
    }

    public boolean startPausing(int timeOut, Set<String> ds, int queueLimit) {
        pauseLock.lock();
        try {
            if (isPausing.get()) {
                return false;
            }
            pauseThreadPool = new PauseEndThreadPool(timeOut, queueLimit);
            lockWithShardingNodes(ds);
            isPausing.set(true);
            return true;
        } finally {
            pauseLock.unlock();
        }
    }

    public void lockWithShardingNodes(Set<String> shardingNodeSet) {
        LOGGER.info("Lock shardingNodes with set size of" + shardingNodeSet.size());
        this.shardingNodes = shardingNodeSet;
        for (Entry<String, SchemaConfig> entry : DbleServer.getInstance().getConfig().getSchemas().entrySet()) {
            if (shardingNodes.contains(entry.getValue().getShardingNode())) {
                LOGGER.info("lock for schema " + entry.getValue().getName() +
                        " shardingNode " + entry.getValue().getShardingNode());
                SchemaConfig schemaConfig = entry.getValue();
                SchemaMeta schemaMeta = ProxyMeta.getInstance().getTmManager().getCatalogs().get(entry.getKey());
                for (Entry<String, TableMeta> tabEntry : schemaMeta.getTableMetas().entrySet()) {
                    if (!schemaConfig.getTables().containsKey(tabEntry.getKey())) {
                        addToLockSet(entry.getKey(), tabEntry.getKey());
                    } else {
                        for (String shardingNode : schemaConfig.getTables().get(tabEntry.getKey()).getShardingNodes()) {
                            if (shardingNodes.contains(shardingNode)) {
                                addToLockSet(entry.getKey(), tabEntry.getKey());
                                break;
                            }
                        }
                    }
                }
            } else {
                SchemaConfig schemaConfig = entry.getValue();
                for (Entry<String, BaseTableConfig> tableEntry : schemaConfig.getTables().entrySet()) {
                    LOGGER.info("lock for schema " + entry.getValue().getName() + " table config ");
                    BaseTableConfig tableConfig = tableEntry.getValue();
                    for (String shardingNode : tableConfig.getShardingNodes()) {
                        if (shardingNodes.contains(shardingNode)) {
                            addToLockSet(entry.getKey(), tableEntry.getKey());
                            break;
                        }
                    }
                }
            }
        }

    }

    private void addToLockSet(String schema, String table) {
        Set<String> tableSet = this.pauseMap.computeIfAbsent(schema, k -> new HashSet<>());
        if (!tableSet.contains(table)) {
            tableSet.add(table);
        }
    }

    public boolean waitForResume(RouteResultset rrs, ShardingService service, String stepNext) {
        pauseLock.lock();
        try {
            return pauseThreadPool.offer(service, stepNext, rrs);
        } finally {
            pauseLock.unlock();
        }
    }

    public void resume() {
        pauseLock.lock();
        try {
            isPausing.set(false);
            shardingNodes = null;
            pauseMap.clear();
            pauseThreadPool.continueExec();
        } finally {
            pauseLock.unlock();
        }
    }


    public boolean tryResume() {
        pauseLock.lock();
        try {
            if (isPausing.compareAndSet(true, false)) {
                shardingNodes = null;
                pauseMap.clear();
                pauseThreadPool.continueExec();
                return true;
            }
            return false;
        } finally {
            pauseLock.unlock();
        }
    }

    public boolean checkTarget(ConcurrentMap<RouteResultsetNode, BackendConnection> target) {
        for (Entry<RouteResultsetNode, BackendConnection> entry : target.entrySet()) {
            if (this.shardingNodes.contains(entry.getKey().getName())) {
                return true;
            }
        }
        return false;
    }

    public boolean checkRRS(RouteResultset rrs) {
        if (!rrs.isNeedOptimizer()) {
            for (RouteResultsetNode node : rrs.getNodes()) {
                if (this.shardingNodes.contains(node.getName())) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    public boolean checkReferredTableNodes(List<TableNode> list) {
        for (TableNode tableNode : list) {
            Set<String> tableSet = this.pauseMap.get(tableNode.getSchema());
            if (tableSet == null) {
                return false;
            }
            if (tableSet.contains(tableNode.getTableName())) {
                return true;
            }
        }
        return false;
    }


    public void clusterPauseNotice(String shardingNode, int timeOut, int queueLimit) throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            final KvBean pauseResultNode = ClusterHelper.getKV(ClusterPathUtil.getPauseResultNodePath());
            if (pauseResultNode != null) {
                if (isSelfPause(pauseResultNode)) {
                    throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "You are paused cluster already");
                } else {
                    throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "Other node in cluster is pausing");
                }
            }
            ClusterHelper.setKV(ClusterPathUtil.getPauseResultNodePath(),
                    new PauseInfo(SystemConfig.getInstance().getInstanceName(), shardingNode, PAUSE, timeOut, queueLimit).toString());
        }
    }


    public boolean isSelfPause(KvBean pauseResultNode) {
        final PauseInfo pauseInfo = new PauseInfo(pauseResultNode.getValue());
        return (pauseInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName()));
    }

    public boolean waitForCluster(ManagerService service, long beginTime, long timeOut) throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            ClusterHelper.createSelfTempNode(ClusterPathUtil.getPauseResultNodePath(), ClusterPathUtil.SUCCESS);
            Map<String, String> expectedMap = ClusterHelper.getOnlineMap();
            StringBuffer sb = new StringBuffer();
            for (; ; ) {
                if (ClusterLogic.checkResponseForOneTime(null, ClusterPathUtil.getPauseResultNodePath(), expectedMap, sb)) {
                    if (sb.length() == 0) {
                        return true;
                    } else {
                        LOGGER.info("wait for cluster error " + sb.toString());
                        service.writeErrMessage(1003, sb.toString());
                        return false;
                    }
                } else if (System.currentTimeMillis() - beginTime > timeOut) {
                    LOGGER.info("wait for cluster timeout, try to resume the self & others");
                    PauseShardingNodeManager.getInstance().resume();
                    service.writeErrMessage(1003, "There are some node in cluster can't recycle backend");
                    PauseShardingNodeManager.getInstance().resumeCluster();
                    return false;
                }
            }
        }
        return true;
    }


    public void resumeCluster() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            ClusterHelper.cleanPath(ClusterPathUtil.getPauseResumePath());

            ClusterHelper.setKV(ClusterPathUtil.getPauseResumePath(),
                    new PauseInfo(SystemConfig.getInstance().getInstanceName(), " ", PauseInfo.RESUME, 0, 0).toString());
            LOGGER.info("try to resume cluster and waiting for others to response");

            ClusterHelper.createSelfTempNode(ClusterPathUtil.getPauseResumePath(), "");
            ClusterLogic.waitingForAllTheNode(ClusterPathUtil.getPauseResumePath(), "");

            ClusterHelper.cleanPath(ClusterPathUtil.getPauseResumePath());
            ClusterHelper.cleanPath(ClusterPathUtil.getPauseResultNodePath());
        }

    }

    public void releaseDistributeLock() {
        if (distributeLock != null) {
            distributeLock.release();
            distributeLock = null;
        }
    }


    public boolean getDistributeLock() {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            DistributeLock templock = ClusterHelper.createDistributeLock(ClusterPathUtil.getPauseShardingNodeLockPath(),
                    SystemConfig.getInstance().getInstanceName());
            if (!templock.acquire()) {
                return false;
            }
            distributeLock = templock;
        }
        return true;
    }

    public Set<String> getShardingNodes() {
        return shardingNodes;
    }
}
