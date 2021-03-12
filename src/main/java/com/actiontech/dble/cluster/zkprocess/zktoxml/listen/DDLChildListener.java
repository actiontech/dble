/*
 * Copyright (C) 2016-2021 ActionTech.
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
        String type = null;
        switch (event.getType()) {
            case CHILD_ADDED:
                type = "created";
                break;
            case CHILD_UPDATED:
                type = "updated";
                break;
            case CHILD_REMOVED:
                type = "deleted";
                break;
            default:
                //ignore other event.
                return;
        }
        ClusterDelayProvider.delayAfterGetDdlNotice();
        ChildData childData = event.getData();

        final String ddlInfoStr = new String(childData.getData(), StandardCharsets.UTF_8);
        DDLInfo ddlInfo = new DDLInfo(ddlInfoStr);


        if (ddlInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            LOGGER.info("DDL node " + childData.getPath() + " is from myself ,so just return ,and data is " + ddlInfo.toString());
            return; //self node
        }

        LOGGER.info("DDL node {} {} , and data is {}", childData.getPath(), type, ddlInfoStr);

        switch (event.getType()) {
            case CHILD_ADDED:
                initMeta(childData, ddlInfo);
                break;
            case CHILD_UPDATED:
                updateMeta(childData, ddlInfo);
                break;
            case CHILD_REMOVED:
                deleteNode(childData, ddlInfo);
                break;
            default:
                break;
        }
    }

    private void initMeta(ChildData childData, DDLInfo ddlInfo) {

        final String childPath = childData.getPath();
        String keyName = childPath.substring(childPath.lastIndexOf("/") + 1);

        ClusterLogic.processStatusEvent(keyName, ddlInfo, DDLStatus.INIT);

        if (DDLStatus.INIT != ddlInfo.getStatus()) {
            LOGGER.warn("get a special CREATE event of zk when doing cluster ddl , status:{}, data is {}", ddlInfo.getStatus(), ddlInfo.toString());
            ClusterLogic.processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus());
        }


    }


    private void updateMeta(ChildData childData, DDLInfo ddlInfo) {
        final String childPath = childData.getPath();
        String keyName = childPath.substring(childPath.lastIndexOf("/") + 1);
        if (DDLStatus.INIT == ddlInfo.getStatus()) {
            //missing DELETE event.
            LOGGER.warn("get a special UPDATE event of zk when doing cluster ddl , status:{}, data is {}", ddlInfo.getStatus(), ddlInfo.toString());
            ClusterLogic.processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus());
        } else {
            // just release local lock
            ClusterLogic.processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus());
        }


    }

    private void deleteNode(ChildData childData, DDLInfo ddlInfo) throws Exception {
        ClusterLogic.deleteDDLNodeEvent(ddlInfo, childData.getPath());
    }


}
