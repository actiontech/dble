/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.DistributeLock;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.logic.ClusterOperation;
import com.oceanbase.obsharding_d.cluster.path.ClusterMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterValue;
import com.oceanbase.obsharding_d.cluster.values.FeedBackType;
import com.oceanbase.obsharding_d.cluster.values.OnlineType;
import com.oceanbase.obsharding_d.cluster.values.PauseInfo;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.meta.PauseEndThreadPool;
import com.oceanbase.obsharding_d.meta.SchemaMeta;
import com.oceanbase.obsharding_d.meta.TableMeta;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.node.TableNode;
import com.oceanbase.obsharding_d.route.RouteResultset;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.services.manager.handler.PacketResult;
import com.oceanbase.obsharding_d.services.mysqlsharding.ShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.oceanbase.obsharding_d.cluster.values.PauseInfo.PAUSE;

public final class PauseShardingNodeManager {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PauseShardingNodeManager.class);
    private static final PauseShardingNodeManager INSTANCE = new PauseShardingNodeManager();
    private ReentrantLock pauseLock = new ReentrantLock();
    private volatile Set<String> shardingNodes = null;
    private Map<String, Set<String>> pauseMap = new ConcurrentHashMap<>();
    private AtomicBoolean isPausing = new AtomicBoolean(false);
    private volatile DistributeLock distributeLock = null;
    private final ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.PAUSE_RESUME);
    private volatile PauseEndThreadPool pauseThreadPool = null;
    private PauseInfo currentParseInfo;

    private PauseShardingNodeManager() {

    }

    public static PauseShardingNodeManager getInstance() {
        return INSTANCE;
    }

    public AtomicBoolean getIsPausing() {
        return this.isPausing;
    }

    public PauseInfo getCurrentParseInfo() {
        return currentParseInfo;
    }

    /**
     * in this implementation, when call this method 'fetchClusterStatus()',the port is already be listened ,but the server has not ready for accept connection yet. So,no FrontConnection will be created, we don't need to call 'waitForSelfPause()' to wait for connection paused;
     *
     * @throws Exception
     */
    public void fetchClusterStatus() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            DistributeLock tempPauseLock = clusterHelper.createDistributeLock(ClusterMetaUtil.getPauseShardingNodeLockPath());
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
                LOGGER.info("fetched Pause lock");
                final PauseInfo pauseInfo = clusterHelper.getPathValue(ClusterMetaUtil.getPauseResultNodePath()).map(ClusterValue::getData).orElse(null);
                if (pauseInfo != null) {
                    //cluster is Pausing
                    LOGGER.info("get pause  value :{}", pauseInfo);
                    Set<String> shardingNodeSet = new HashSet<>(Arrays.asList(pauseInfo.getShardingNodes().split(",")));

                    //pause self
                    startPausing(pauseInfo.getConnectionTimeOut(), shardingNodeSet, pauseInfo.getShardingNodes(), pauseInfo.getQueueLimit());
                }
                //cluster is resuming.
                //just return;
                return;
            } finally {
                if (locked) {
                    tempPauseLock.release();
                    LOGGER.info("released pause lock");
                }

            }
        }
    }

    /**
     * @param timeOut
     * @param ds
     * @param dsStr
     * @param queueLimit
     * @throws MySQLOutPutException throws only when single mod.
     */
    public void startPausing(int timeOut, Set<String> ds, String dsStr, int queueLimit) throws MySQLOutPutException {
        pauseLock.lock();
        try {
            if (isPausing.get()) {
                //the error message can only show in single mod
                if ((!Objects.equals(dsStr, currentParseInfo.getShardingNodes())) || (!Objects.equals(timeOut, currentParseInfo.getConnectionTimeOut()) || (!Objects.equals(queueLimit, currentParseInfo.getQueueLimit())))) {
                    throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "You can't run different PAUSE commands at the same time. Please resume previous PAUSE command first.");
                } else {
                    throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "You are paused already");
                }
            }
            currentParseInfo = new PauseInfo(SystemConfig.getInstance().getInstanceName(), dsStr, PAUSE, timeOut, queueLimit);
            pauseThreadPool = new PauseEndThreadPool(timeOut, queueLimit);
            lockWithShardingNodes(ds);
            isPausing.set(true);
        } finally {
            pauseLock.unlock();
        }
    }

    public void lockWithShardingNodes(Set<String> shardingNodeSet) {
        LOGGER.info("Lock shardingNodes with set size of {}", shardingNodeSet.size());
        this.shardingNodes = shardingNodeSet;
        for (Entry<String, SchemaConfig> entry : OBsharding_DServer.getInstance().getConfig().getSchemas().entrySet()) {
            SchemaConfig schemaConfig = entry.getValue();
            if (entry.getValue().getDefaultShardingNodes() != null &&
                    !Collections.disjoint(shardingNodes, schemaConfig.getDefaultShardingNodes())) {
                LOGGER.info("lock for schema {} shardingNode {}", entry.getValue().getName(), String.join(",", schemaConfig.getDefaultShardingNodes()));
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
                for (Entry<String, BaseTableConfig> tableEntry : schemaConfig.getTables().entrySet()) {
                    LOGGER.info("lock for schema {} table config ", entry.getValue().getName());
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
                LOGGER.debug("resume self done with return value {}", true);
                return true;
            }
            LOGGER.debug("resume self done with return value {}", false);
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
            final PauseInfo pauseInfo = clusterHelper.getPathValue(ClusterMetaUtil.getPauseResultNodePath()).map(ClusterValue::getData).orElse(null);
            if (pauseInfo != null) {
                if ((!Objects.equals(shardingNode, pauseInfo.getShardingNodes())) || (!Objects.equals(timeOut, pauseInfo.getConnectionTimeOut()) || (!Objects.equals(queueLimit, pauseInfo.getQueueLimit())))) {
                    throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "You can't run different PAUSE commands at the same time. Please resume previous PAUSE command first.");
                }
                if (isSelfPause(pauseInfo)) {
                    throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "You are paused cluster already");
                } else {
                    throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "Other node in cluster is pausing");
                }
            }
            clusterHelper.setKV(ClusterMetaUtil.getPauseResultNodePath(),
                    new PauseInfo(SystemConfig.getInstance().getInstanceName(), shardingNode, PAUSE, timeOut, queueLimit));
            LOGGER.debug("set cluster status for notice done.");
        }
    }


    public boolean isSelfPause(PauseInfo pauseInfo) {
        return (pauseInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName()));
    }

    public boolean waitForCluster(long beginTime, long timeOut, PacketResult packetResult) throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            clusterHelper.createSelfTempNode(ClusterPathUtil.getPauseResultNodePath(), FeedBackType.SUCCESS);
            Map<String, OnlineType> expectedMap = ClusterHelper.getOnlineMap();
            StringBuilder sb = new StringBuilder();
            for (; ; ) {
                if (ClusterLogic.forPauseResume().checkResponseForOneTime(ClusterPathUtil.getPauseResultNodePath(), expectedMap, sb)) {
                    if (sb.length() == 0) {
                        return true;
                    } else {
                        LOGGER.info("wait for cluster error " + sb.toString());
                        packetResult.setSuccess(false);
                        packetResult.setErrorMsg(sb.toString());
                        packetResult.setErrorCode(1003);
                        return false;
                    }
                } else if (System.currentTimeMillis() - beginTime > timeOut) {
                    LOGGER.info("wait for cluster timeout, try to resume the self & others");
                    PauseShardingNodeManager.getInstance().resume();
                    //increase the count before write to client. so , the cluster detach won't work.
                    AbstractGeneralListener.getDoingCount().incrementAndGet();
                    try {
                        packetResult.setSuccess(false);
                        packetResult.setErrorMsg("There are some node in cluster doesn't complete the task. we will try to resume cluster in the backend, please check the OBsharding-D status and OBsharding-D log");
                        packetResult.setErrorCode(1003);
                        //resume in the backends
                        PauseShardingNodeManager.getInstance().resumeCluster();
                    } catch (Exception e) {
                        //we can't throw this exception , because client is received error message before resume. we can't send two error message to client.
                        LOGGER.error("resume cluster operation encounter an error: ", e);
                    } finally {
                        AbstractGeneralListener.getDoingCount().decrementAndGet();
                    }
                    return false;
                }
            }
        }
        return true;
    }


    public void resumeCluster() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            ClusterHelper.cleanPath(ClusterPathUtil.getPauseResumePath());

            clusterHelper.setKV(ClusterMetaUtil.getPauseResumePath(),
                    new PauseInfo(SystemConfig.getInstance().getInstanceName(), " ", PauseInfo.RESUME, 0, 0));
            LOGGER.info("try to resume cluster and waiting for others to response");

            clusterHelper.createSelfTempNode(ClusterPathUtil.getPauseResumePath(), FeedBackType.SUCCESS);
            ClusterLogic.forPauseResume().waitingForAllTheNode(ClusterPathUtil.getPauseResumePath());

            ClusterHelper.cleanPath(ClusterPathUtil.getPauseResumePath());
            ClusterHelper.cleanPath(ClusterPathUtil.getPauseResultNodePath());
            LOGGER.debug("resumed cluster");
        }

    }

    public void releaseDistributeLock() {
        if (distributeLock != null) {
            distributeLock.release();
            //not take effect immediately
            LOGGER.debug("release Pause lock");
            distributeLock = null;
        }
    }


    public boolean getDistributeLock() {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            DistributeLock templock = clusterHelper.createDistributeLock(ClusterMetaUtil.getPauseShardingNodeLockPath());
            if (!templock.acquire()) {
                return false;
            }
            LOGGER.debug("fetched Pause lock");
            distributeLock = templock;
        }
        return true;
    }

    public Set<String> getShardingNodes() {
        return shardingNodes;
    }
}
