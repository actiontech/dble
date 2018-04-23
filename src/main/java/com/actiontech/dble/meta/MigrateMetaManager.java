package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.TableConfig;
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

public class MigrateMetaManager {
    protected static final Logger LOGGER = LoggerFactory.getLogger(MigrateMetaManager.class);
    private ReentrantLock metaLock = new ReentrantLock();
    private Condition condRelease = this.metaLock.newCondition();
    private Set<String> dataNodes = null;
    private Map<String, Set<String>> pauseMap = new ConcurrentHashMap();
    private AtomicBoolean isPausing = new AtomicBoolean(false);

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
}
