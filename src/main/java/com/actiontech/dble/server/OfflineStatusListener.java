/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.BinlogPause;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.TimeUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class OfflineStatusListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(OfflineStatusListener.class);

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData childData = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED:
                break;
            case CHILD_UPDATED:
                break;
            case CHILD_REMOVED:
                deleteNode(childData);
                break;
            default:
                break;
        }
    }

    private void deleteNode(ChildData childData) {
        String nodeName = childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
        releaseForDDL(nodeName);
        releaseForBinlog(nodeName);
    }

    private void releaseForDDL(String crashNode) {
        String ddlPath = KVPathUtil.getDDLPath();
        CuratorFramework zkConn = ZKUtils.getConnection();
        try {
            List<String> ddlList = zkConn.getChildren().forPath(ddlPath);
            if (ddlList == null) {
                return;
            }
            for (String ddlNode : ddlList) {
                String ddlNodePath = ZKPaths.makePath(ddlPath, ddlNode);
                byte[] ddlData = zkConn.getData().forPath(ddlNodePath);
                String data = new String(ddlData, StandardCharsets.UTF_8);
                DDLInfo ddlInfo = new DDLInfo(data);
                if (!ddlInfo.getFrom().equals(crashNode)) {
                    continue;
                }
                if (DDLInfo.DDLStatus.INIT == ddlInfo.getStatus()) {
                    String[] tableInfo = ddlNode.split("\\.");
                    String schema = StringUtil.removeBackQuote(tableInfo[0]);
                    String table = StringUtil.removeBackQuote(tableInfo[1]);
                    DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
                    LOGGER.info(" service instance[" + crashNode + "] has crashed. Remove MetaLock for " + ddlNode);
                }
                //other status should be unlocked
                LOGGER.warn(" service instance[" + crashNode + "] has crashed." +
                        "Please manually check ddl status on every data node and delete ddl node [" + ddlNodePath + "]  from zookeeper " +
                        "after every instance received this message");
            }
        } catch (Exception e) {
            LOGGER.warn(" releaseForDDL error", e);
        }
    }

    private void releaseForBinlog(String crashNode) {
        String binlogStatusPath = KVPathUtil.getBinlogPauseStatus();
        CuratorFramework zkConn = ZKUtils.getConnection();
        try {
            byte[] binlogStatusData = zkConn.getData().forPath(binlogStatusPath);
            if (binlogStatusData == null) {
                return;
            }
            String data = new String(binlogStatusData, StandardCharsets.UTF_8);
            BinlogPause binlogPauseInfo = new BinlogPause(data);
            if (!binlogPauseInfo.getFrom().equals(crashNode)) {
                return;
            }
            if (BinlogPause.BinlogPauseStatus.ON == binlogPauseInfo.getStatus()) {
                //ClusterParamCfg.CLUSTER_CFG_MYID
                String instancePath = ZKPaths.makePath(KVPathUtil.getBinlogPauseInstance(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
                boolean needDelete = true;
                long beginTime = TimeUtil.currentTimeMillis();
                long timeout = DbleServer.getInstance().getConfig().getSystem().getShowBinlogStatusTimeout();
                while (zkConn.checkExists().forPath(instancePath) == null) {
                    //wait 2* timeout to release itself
                    if (TimeUtil.currentTimeMillis() > beginTime + 2 * timeout) {
                        LOGGER.warn("checkExists of " + instancePath + " time out");
                        needDelete = false;
                        break;
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                }
                if (needDelete) {
                    try {
                        zkConn.delete().forPath(instancePath);
                    } catch (Exception e) {
                        LOGGER.warn(" delete binlogPause instance failed", e);
                    }
                }
                DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
            }
            LOGGER.warn(" service instance[" + crashNode + "] has crashed." +
                    "Please manually make sure node [" + binlogStatusPath + "] status in zookeeper " +
                    "after every instance received this message");
        } catch (Exception e) {
            LOGGER.warn(" releaseForBinlog error", e);
        }
    }
}
