/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.logic;

import com.oceanbase.obsharding_d.btrace.provider.ClusterDelayProvider;
import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.path.ChildPathMeta;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.*;
import com.oceanbase.obsharding_d.meta.DDLProxyMetaManager;
import com.oceanbase.obsharding_d.singleton.DDLTraceHelper;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public class DDLClusterLogic extends AbstractClusterLogic {
    private static final Logger LOGGER = LogManager.getLogger(DDLClusterLogic.class);
    private static Map<String, String> ddlLockMap = new ConcurrentHashMap<>();

    DDLClusterLogic() {
        super(ClusterOperation.DDL);
    }

    public void processStatusEvent(String keyName, DDLInfo ddlInfo, DDLInfo.DDLStatus status, String path) {
        try {
            switch (status) {
                case INIT:
                    this.ddlInitEvent(keyName, ddlInfo, path);
                    break;
                case SUCCESS:
                    // just release local lock
                    this.ddlSuccessEvent(keyName, ddlInfo, path);
                    break;
                case FAILED:
                    // just release local lock
                    this.ddlFailedEvent(keyName, path);
                    break;

                default:
                    break;

            }
        } catch (Exception e) {
            LOGGER.warn("Error when update the meta data of the DDL " + ddlInfo.toString(), e);
        }

    }

    private void ddlInitEvent(String keyName, DDLInfo ddlInfo, String path) throws Exception {
        String[] tableInfo = keyName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        String fullName = schema + "." + table;
        ddlLockMap.put(fullName, ddlInfo.getFrom());
        DDLTraceHelper.log2(null, DDLTraceHelper.Stage.receive_ddl_prepare, "Received: initialize ddl{" + ddlInfo.getSql() + "} of table[" + fullName + "]");
        boolean metaLocked = false;
        try {
            DDLProxyMetaManager.Subscriber.addTableMetaLock(schema, table, ddlInfo.getSql());
            metaLocked = true;
            clusterHelper.createSelfTempNode(path, FeedBackType.SUCCESS);
        } catch (Exception t) {
            DDLProxyMetaManager.Subscriber.removeTableMetaLock(schema, table);
            if (!metaLocked) {
                clusterHelper.createSelfTempNode(path, FeedBackType.ofError(t.getMessage()));
            }
            throw t;
        }
    }

    private void ddlSuccessEvent(String keyName, DDLInfo ddlInfo, String path) throws Exception {
        String[] tableInfo = keyName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        String fullName = schema + "." + table;
        DDLTraceHelper.log2(null, DDLTraceHelper.Stage.receive_ddl_complete, "Received: ddl execute success notice for table[" + fullName + "]");
        // if the start node is done the ddl execute
        ddlLockMap.remove(fullName);
        ClusterDelayProvider.delayBeforeUpdateMeta();
        DDLProxyMetaManager.Subscriber.updateMetaData(schema, table, ddlInfo, true);
        ClusterDelayProvider.delayBeforeDdlResponse();
        clusterHelper.createSelfTempNode(path, FeedBackType.SUCCESS);
    }

    private void ddlFailedEvent(String keyName, String path) throws Exception {
        String[] tableInfo = keyName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        String fullName = schema + "." + table;
        DDLTraceHelper.log2(null, DDLTraceHelper.Stage.receive_ddl_complete, "Received: ddl execute failed notice for table[" + fullName + "]");
        //if the start node executing ddl with error,just release the lock
        ddlLockMap.remove(fullName);
        DDLProxyMetaManager.Subscriber.updateMetaData(schema, table, null, false);
        clusterHelper.createSelfTempNode(path, FeedBackType.SUCCESS);
    }

    public void deleteDDLNodeEvent(DDLInfo ddlInfo, String path) throws Exception {
        LOGGER.info("DDL node " + path + " removed , and DDL info is " + ddlInfo.toString());
    }

    public void checkDDLAndRelease(String crashNode) {
        //deal with the status when the ddl is init notified
        //and than the ddl server is shutdown
        Set<String> tableToDel = new HashSet<>();
        for (Map.Entry<String, String> en : ddlLockMap.entrySet()) {
            if (crashNode.equals(en.getValue())) {
                String fullName = en.getKey();
                String[] tableInfo = fullName.split("\\.");
                final String schema = StringUtil.removeBackQuote(tableInfo[0]);
                final String table = StringUtil.removeBackQuote(tableInfo[1]);
                DDLProxyMetaManager.Subscriber.removeTableMetaLock(schema, table);
                tableToDel.add(fullName);
                ddlLockMap.remove(fullName);
            }
        }
        if (tableToDel.size() > 0) {
            try {
                List<ClusterEntry<AnyType>> kvs = this.getKVBeanOfChildPath(ChildPathMeta.of(ClusterPathUtil.getDDLPath(), AnyType.class));
                for (ClusterEntry<?> kv : kvs) {
                    String path = kv.getKey();
                    String[] paths = path.split(ClusterPathUtil.SEPARATOR);
                    String keyName = paths[paths.length - 1];
                    String[] tableInfo = keyName.split("\\.");
                    final String schema = StringUtil.removeBackQuote(tableInfo[0]);
                    final String table = StringUtil.removeBackQuote(tableInfo[1]);
                    String fullName = schema + "." + table;
                    if (tableToDel.contains(fullName)) {
                        ClusterHelper.cleanPath(path);
                    }
                }
            } catch (Exception e) {
                LOGGER.warn(" service instance[" + crashNode + "] has crashed. " +
                        "Please manually check ddl status on cluster and delete ddl path[" + tableToDel + "] from cluster ");
            }
        }
    }
}
