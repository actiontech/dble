/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkParamCfg;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo.DDLStatus;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by huqing.yan on 2017/6/6.
 */
public class DDLChildListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(DDLChildListener.class);

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData childData = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED:
                try {
                    lockTableByNewNode(childData);
                } catch (Exception e) {
                    LOGGER.warn("CHILD_ADDED error", e);
                }
                break;
            case CHILD_UPDATED:
                updateMeta(childData);
                break;
            case CHILD_REMOVED:
                deleteNode(childData);
                break;
            default:
                break;
        }
    }

    private void lockTableByNewNode(ChildData childData) throws Exception {
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        LOGGER.info("DDL node " + childData.getPath() + " created , and data is " + data);
        DDLInfo ddlInfo = new DDLInfo(data);
        final String fromNode = ddlInfo.getFrom();
        if (fromNode.equals(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID))) {
            return; //self node
        }
        if (DDLStatus.INIT != ddlInfo.getStatus()) {
            return;
        }
        String nodeName = childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
        String[] tableInfo = nodeName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        final String ddl = ddlInfo.getSql();
        final String tablePath = childData.getPath();
        try {
            DbleServer.getInstance().getTmManager().addMetaLock(schema, table);
            //monitor if the from node is crash
            new Thread(new Runnable() {
                @Override
                public void run() {
                    CuratorFramework zkConn = ZKUtils.getConnection();
                    try {
                        while (zkConn.checkExists().forPath(tablePath) != null) {
                            List<String> onlineList = zkConn.getChildren().forPath(KVPathUtil.getOnlinePath());
                            if (!onlineList.contains(fromNode) && DbleServer.getInstance().getTmManager().isMetaLocked(schema, table)) {
                                LOGGER.warn("mode [" + fromNode + "] is not online, but ddl [" + ddl + "] is not finished,so you may need to check table status and reload meta data");
                                DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
                                break;
                            } else {
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.warn("checkPath error", e);
                    }
                }
            }).start();
        } catch (Exception t) {
            DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
            throw t;
        }
    }

    private void updateMeta(ChildData childData) {
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        LOGGER.info("DDL node " + childData.getPath() + " updated , and data is " + data);
        DDLInfo ddlInfo = new DDLInfo(data);
        if (ddlInfo.getFrom().equals(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID))) {
            return; //self node
        }
        if (DDLStatus.INIT == ddlInfo.getStatus()) {
            return;
        }
        DbleServer.getInstance().getTmManager().updateMetaData(ddlInfo.getSchema(), ddlInfo.getSql(), DDLStatus.SUCCESS.equals(ddlInfo.getStatus()), false);
    }

    private void deleteNode(ChildData childData) {
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        DDLInfo ddlInfo = new DDLInfo(data);
        LOGGER.info("DDL node " + childData.getPath() + " removed , and DDL info is " + ddlInfo.toString());
    }
}
