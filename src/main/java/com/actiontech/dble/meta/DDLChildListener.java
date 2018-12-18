/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.DDLInfo.DDLStatus;
import com.actiontech.dble.util.StringUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

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
        if (fromNode.equals(ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
            return; //self node
        }
        if (DDLStatus.INIT != ddlInfo.getStatus()) {
            return;
        }
        String nodeName = childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
        String[] tableInfo = nodeName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        try {
            DbleServer.getInstance().getTmManager().addMetaLock(schema, table, ddlInfo.getSql());
        } catch (Exception t) {
            DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
            throw t;
        }
    }

    private void updateMeta(ChildData childData) {
        String nodeName = childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
        String[] tableInfo = nodeName.split("\\.");
        final String table = StringUtil.removeBackQuote(tableInfo[1]);

        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        LOGGER.info("DDL node " + childData.getPath() + " updated , and data is " + data);
        DDLInfo ddlInfo = new DDLInfo(data);
        if (ddlInfo.getFrom().equals(ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
            return; //self node
        }
        if (DDLStatus.INIT == ddlInfo.getStatus()) {
            return;
        }
        //to judge the table is be drop
        if (ddlInfo.getType() == DDLInfo.DDLType.DROP_TABLE) {
            DbleServer.getInstance().getTmManager().updateMetaData(ddlInfo.getSchema(), ddlInfo.getSql(), DDLInfo.DDLStatus.SUCCESS.equals(ddlInfo.getStatus()), false);
        } else {
            //else get the lastest table meta from db
            DbleServer.getInstance().getTmManager().updateOnetableWithBackData(DbleServer.getInstance().getConfig(), ddlInfo.getSchema(), table);
            try {
                DbleServer.getInstance().getTmManager().notifyResponseClusterDDL(ddlInfo.getSchema(), table, ddlInfo.getSql(), DDLInfo.DDLStatus.SUCCESS, ddlInfo.getType(), false);
            } catch (Exception e) {
                LOGGER.info("Error when update the meta data of the DDL " + ddlInfo.toString());
            }
        }
    }

    private void deleteNode(ChildData childData) {
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        DDLInfo ddlInfo = new DDLInfo(data);
        LOGGER.info("DDL node " + childData.getPath() + " removed , and DDL info is " + ddlInfo.toString());
    }
}
