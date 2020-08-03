/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.DDLInfo;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.DDLInfo.DDLStatus;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.ProxyMeta;
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
        ClusterDelayProvider.delayAfterGetDdlNotice();
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
        if (fromNode.equals(SystemConfig.getInstance().getInstanceId())) {
            return; //self node
        }
        if (DDLStatus.INIT != ddlInfo.getStatus()) {
            return;
        }
        String nodeName = childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
        String[] tableInfo = nodeName.split("\\.");
        final String schema = StringUtil.removeBackQuote(tableInfo[0]);
        final String table = StringUtil.removeBackQuote(tableInfo[1]);
        ClusterDelayProvider.delayBeforeUpdateMeta();
        try {
            ProxyMeta.getInstance().getTmManager().addMetaLock(schema, table, ddlInfo.getSql());
        } catch (Exception t) {
            ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
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
        if (ddlInfo.getFrom().equals(SystemConfig.getInstance().getInstanceId())) {
            return; //self node
        }
        if (DDLStatus.INIT == ddlInfo.getStatus()) {
            return;
        }
        ClusterDelayProvider.delayBeforeUpdateMeta();
        // just release local lock
        if (ddlInfo.getStatus() == DDLStatus.FAILED) {
            ProxyMeta.getInstance().getTmManager().removeMetaLock(ddlInfo.getSchema(), table);
            try {
                ProxyMeta.getInstance().getTmManager().notifyResponseClusterDDL(ddlInfo.getSchema(), table, ddlInfo.getSql(), DDLInfo.DDLStatus.FAILED, ddlInfo.getType(), false);
            } catch (Exception e) {
                LOGGER.info("Error when update the meta data of the DDL " + ddlInfo.toString());
            }
            return;
        }
        //to judge the table is be drop
        if (ddlInfo.getType() == DDLInfo.DDLType.DROP_TABLE) {
            ProxyMeta.getInstance().getTmManager().updateMetaData(ddlInfo.getSchema(), table, ddlInfo.getSql(), DDLInfo.DDLStatus.SUCCESS.equals(ddlInfo.getStatus()), false, ddlInfo.getType());
        } else {
            //else get the latest table meta from db
            ProxyMeta.getInstance().getTmManager().updateOnetableWithBackData(DbleServer.getInstance().getConfig(), ddlInfo.getSchema(), table);
            ClusterDelayProvider.delayBeforeDdlResponse();
            try {
                ProxyMeta.getInstance().getTmManager().notifyResponseClusterDDL(ddlInfo.getSchema(), table, ddlInfo.getSql(), DDLInfo.DDLStatus.SUCCESS, ddlInfo.getType(), false);
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
