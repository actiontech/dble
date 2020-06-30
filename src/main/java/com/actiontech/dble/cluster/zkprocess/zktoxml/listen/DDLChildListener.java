/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.values.DDLInfo;
import com.actiontech.dble.cluster.values.DDLInfo.DDLStatus;
import com.actiontech.dble.config.model.SystemConfig;
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("event happen:" + event.toString());
        }
        ClusterDelayProvider.delayAfterGetDdlNotice();
        ChildData childData = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED:
                try {
                    initDDL(childData);
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

    private void initDDL(ChildData childData) throws Exception {
        String childPath = childData.getPath();
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        DDLInfo ddlInfo = new DDLInfo(data);

        LOGGER.info("DDL node " + childData.getPath() + " created , and data is " + data);
        if (ddlInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            LOGGER.info("DDL node " + childPath + " is from myself ,so just return ,and data is " + data);
            return; //self node
        }
        if (DDLStatus.INIT != ddlInfo.getStatus()) {
            return;
        }
        String keyName = childPath.substring(childPath.lastIndexOf("/") + 1);
        ClusterLogic.initDDLEvent(keyName, ddlInfo);
    }

    private void updateMeta(ChildData childData) {
        String childPath = childData.getPath();
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        DDLInfo ddlInfo = new DDLInfo(data);
        if (ddlInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            LOGGER.info("DDL node " + childData.getPath() + " is from myself ,so just return ,and data is " + data);
            return; //self node
        }
        if (DDLStatus.INIT == ddlInfo.getStatus()) {
            return;
        }
        String keyName = childPath.substring(childPath.lastIndexOf("/") + 1);
        LOGGER.info("DDL node " + childPath + " updated , and data is " + ddlInfo);
        // just release local lock
        if (ddlInfo.getStatus() == DDLStatus.FAILED) {
            try {
                ClusterLogic.ddlFailedEvent(keyName);
            } catch (Exception e) {
                LOGGER.info("Error when update the meta data of the DDL " + ddlInfo.toString());
            }
        } else {
            try {
                ClusterLogic.ddlUpdateEvent(keyName, ddlInfo);
            } catch (Exception e) {
                LOGGER.info("Error when update the meta data of the DDL " + ddlInfo.toString());
            }
        }
    }

    private void deleteNode(ChildData childData) throws Exception {
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        DDLInfo ddlInfo = new DDLInfo(data);
        ClusterLogic.deleteDDLNodeEvent(ddlInfo, childData.getPath());
    }
}
