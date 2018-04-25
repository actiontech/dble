package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UDistributeLock;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.PauseInfo;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.meta.protocol.StructureMeta;
import com.actiontech.dble.plan.node.TableNode;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.actiontech.dble.config.loader.zkprocess.zookeeper.process.PauseInfo.PAUSE;

public class MigrateMetaManager {
    protected static final Logger LOGGER = LoggerFactory.getLogger(MigrateMetaManager.class);
    private ReentrantLock metaLock = new ReentrantLock();
    private Condition condRelease = this.metaLock.newCondition();
    private Set<String> dataNodes = null;
    private Map<String, Set<String>> pauseMap = new ConcurrentHashMap();
    private AtomicBoolean isPausing = new AtomicBoolean(false);
    private UDistributeLock uDistributeLock = null;

    public AtomicBoolean getIsPausing() {
        return this.isPausing;
    }

    public void lockWithDataNodes(Set<String> dataNodeSet) {
        this.dataNodes = dataNodeSet;
        for (Entry<String, SchemaConfig> entry : DbleServer.getInstance().getConfig().getSchemas().entrySet()) {
            if (dataNodes.contains(entry.getValue().getDataNode())) {
                SchemaConfig schemaConfig = entry.getValue();
                SchemaMeta schemaMeta = DbleServer.getInstance().getTmManager().getCatalogs().get(entry.getKey());
                for (Entry<String, StructureMeta.TableMeta> tabEntry : schemaMeta.getTableMetas().entrySet()) {
                    if (!schemaConfig.getTables().containsKey(tabEntry.getKey())) {
                        addToLockSet(entry.getKey(), tabEntry.getKey());
                    } else {
                        for (String dataNode : schemaConfig.getTables().get(tabEntry.getKey()).getDataNodes()) {
                            if (dataNodes.contains(dataNode)) {
                                addToLockSet(entry.getKey(), tabEntry.getKey());
                                break;
                            }
                        }
                    }
                }
            } else {
                SchemaConfig schemaConfig = entry.getValue();
                for (Entry<String, TableConfig> tableEntry : schemaConfig.getTables().entrySet()) {
                    TableConfig tableConfig = tableEntry.getValue();
                    for (String dataNode : tableConfig.getDataNodes()) {
                        if (dataNodes.contains(dataNode)) {
                            addToLockSet(entry.getKey(), tableEntry.getKey());
                            break;
                        }
                    }
                }
            }
        }

    }

    private void addToLockSet(String schema, String table) {
        Set<String> tableSet = (Set) this.pauseMap.get(schema);
        if (tableSet == null) {
            tableSet = new HashSet();
            this.pauseMap.put(schema, tableSet);
        }
        if (!tableSet.contains(table)) {
            tableSet.add(table);
        }
    }

    public void waitForResume() {
        metaLock.lock();
        try {
            while (isPausing.get()) {
                condRelease.await();
            }
        } catch (Exception e) {
            LOGGER.info("await error ");
        } finally {
            metaLock.unlock();
        }
    }

    public void resume() {
        metaLock.lock();
        try {
            isPausing.set(false);
            dataNodes = null;
            pauseMap = new ConcurrentHashMap();
            condRelease.signalAll();
        } finally {
            metaLock.unlock();
        }
    }

    public boolean checkTarget(ConcurrentMap<RouteResultsetNode, BackendConnection> target) {
        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : target.entrySet()) {
            if (this.dataNodes.contains(entry.getKey().getName())) {
                return true;
            }
        }
        return false;
    }

    public boolean checkRRS(RouteResultset rrs) {
        if (!rrs.isNeedOptimizer()) {
            for (RouteResultsetNode node : rrs.getNodes()) {
                if (this.dataNodes.contains(node.getName())) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    public boolean checkReferedTableNodes(List<TableNode> list) {
        for (TableNode tableNode : list) {
            Set<String> tableSet = (Set) this.pauseMap.get(tableNode.getSchema());
            if (tableSet == null) {
                break;
            }
            if (tableSet.contains(tableNode.getTableName())) {
                return true;
            }
        }
        return false;
    }


    public boolean clusterPauseNotic(String dataNode) {
        if (DbleServer.getInstance().isUseUcore()) {
            uDistributeLock = new UDistributeLock(UcorePathUtil.getPauseDataNodePath(),
                    new PauseInfo(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), dataNode, PAUSE).toString());
            if (!uDistributeLock.acquire()) {
                return false;
            }
        }
        return true;
    }


    public boolean waitForCluster(ManagerConnection c, long beginTime, long timeOut) {

        LOGGER.info("DEBUG of sunzhengfang " + DbleServer.getInstance().isUseUcore());
        if (DbleServer.getInstance().isUseUcore()) {
            for (; ; ) {
                List<UKvBean> reponseList = ClusterUcoreSender.getKeyTree(UcorePathUtil.getPauseResultNodePath());
                List<UKvBean> onlineList = ClusterUcoreSender.getKeyTree(UcorePathUtil.getOnlinePath());
                LOGGER.info("DEBUG of sunzhengfang " + reponseList.size() + "  " + onlineList.size());
                if (reponseList.size() >= onlineList.size() - 1) {
                    return true;
                } else if (System.currentTimeMillis() - beginTime > timeOut) {
                    c.writeErrMessage(1003, "There are some node in cluster can recycle backend");
                    return false;
                }
            }
        }
        return true;
    }

    public UDistributeLock getuDistributeLock() {
        return uDistributeLock;
    }

    public void setuDistributeLock(UDistributeLock uDistributeLock) {
        this.uDistributeLock = uDistributeLock;
    }
}
