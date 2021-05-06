/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.logic;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ChildPathMeta;
import com.actiontech.dble.cluster.ClusterEntry;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.values.AnyType;
import com.actiontech.dble.cluster.values.DDLInfo;
import com.actiontech.dble.cluster.values.FeedBackType;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.util.StringUtil;
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


    public void initDDLEvent(String keyName, DDLInfo ddlInfo) throws Exception {
        String[] tableInfo = keyName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        String fullName = schema + "." + table;
        ddlLockMap.put(fullName, ddlInfo.getFrom());
        LOGGER.info("init of ddl " + schema + " " + table);
        try {
            ProxyMeta.getInstance().getTmManager().addMetaLock(schema, table, ddlInfo.getSql());
        } catch (Exception t) {
            ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
            throw t;
        }
    }

    public void processStatusEvent(String keyName, DDLInfo ddlInfo, DDLInfo.DDLStatus status) {
        try {
            switch (status) {
                case INIT:
                    this.initDDLEvent(keyName, ddlInfo);
                    break;
                case SUCCESS:
                    // just release local lock
                    this.ddlUpdateEvent(keyName, ddlInfo);
                    break;
                case FAILED:
                    // just release local lock
                    this.ddlFailedEvent(keyName);
                    break;

                default:
                    break;

            }
        } catch (Exception e) {
            LOGGER.info("Error when update the meta data of the DDL " + ddlInfo.toString());
        }

    }

    public void deleteDDLNodeEvent(DDLInfo ddlInfo, String path) throws Exception {
        LOGGER.info("DDL node " + path + " removed , and DDL info is " + ddlInfo.toString());
    }

    public void ddlFailedEvent(String keyName) throws Exception {
        String[] tableInfo = keyName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        String fullName = schema + "." + table;
        LOGGER.info("ddl execute failed notice, table is " + fullName);
        //if the start node executing ddl with error,just release the lock
        ddlLockMap.remove(fullName);
        ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
        clusterHelper.createSelfTempNode(ClusterPathUtil.getDDLPath(fullName), FeedBackType.SUCCESS);
    }

    public void ddlUpdateEvent(String keyName, DDLInfo ddlInfo) throws Exception {
        LOGGER.info("ddl execute success notice");
        String[] tableInfo = keyName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        String fullName = schema + "." + table;
        // if the start node is done the ddl execute
        ddlLockMap.remove(fullName);
        ClusterDelayProvider.delayBeforeUpdateMeta();
        //to judge the table is be drop
        if (ddlInfo.getType() == DDLInfo.DDLType.DROP_TABLE) {
            ProxyMeta.getInstance().getTmManager().dropTable(schema, table, ddlInfo.getSql(), DDLInfo.DDLStatus.SUCCESS.equals(ddlInfo.getStatus()), false);
        } else {
            //else get the latest table meta from db
            ProxyMeta.getInstance().getTmManager().updateOnetableWithBackData(DbleServer.getInstance().getConfig(), schema, table);
        }

        ClusterDelayProvider.delayBeforeDdlResponse();
        clusterHelper.createSelfTempNode(ClusterPathUtil.getDDLPath(fullName), FeedBackType.SUCCESS);
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
                ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
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
