/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.ClusterEvent;
import com.actiontech.dble.cluster.ClusterValue;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.values.DDLInfo;
import com.actiontech.dble.cluster.values.DDLInfo.DDLStatus;
import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huqing.yan on 2017/6/6.
 */
public class DDLChildListener extends AbstractGeneralListener<DDLInfo> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DDLChildListener.class);

    public DDLChildListener() {
        super(ClusterChildMetaUtil.getDDLPath());
    }

    @Override
    public void onEvent(ClusterEvent<DDLInfo> event) throws Exception {
        ClusterDelayProvider.delayAfterGetDdlNotice();
        final ClusterValue<DDLInfo> value = event.getValue();

        DDLInfo ddlInfo = value.getData();


        if (ddlInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            LOGGER.info("DDL node " + event.getPath() + " is from myself ,so just return ,and data is " + ddlInfo.toString());
            return; //self node
        }

        LOGGER.info("DDL node {} {} , and data is {}", event.getPath(), event.getChangeType(), ddlInfo);

        switch (event.getChangeType()) {
            case ADDED:
                initMeta(event, ddlInfo);
                break;
            case UPDATED:
                updateMeta(event, ddlInfo);
                break;
            case REMOVED:
                deleteNode(event, ddlInfo);
                break;
            default:
                break;
        }
    }

    private void initMeta(ClusterEvent<?> event, DDLInfo ddlInfo) {

        final String childPath = event.getPath();
        String keyName = childPath.substring(childPath.lastIndexOf("/") + 1);

        ClusterLogic.forDDL().processStatusEvent(keyName, ddlInfo, DDLStatus.INIT);

        if (DDLStatus.INIT != ddlInfo.getStatus()) {
            LOGGER.warn("get a special CREATE event of zk when doing cluster ddl , status:{}, data is {}", ddlInfo.getStatus(), ddlInfo.toString());
            ClusterLogic.forDDL().processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus());
        }


    }


    private void updateMeta(ClusterEvent<?> event, DDLInfo ddlInfo) {
        final String childPath = event.getPath();
        String keyName = childPath.substring(childPath.lastIndexOf("/") + 1);
        if (DDLStatus.INIT == ddlInfo.getStatus()) {
            //missing DELETE event.
            LOGGER.warn("get a special UPDATE event of zk when doing cluster ddl , status:{}, data is {}", ddlInfo.getStatus(), ddlInfo.toString());
            ClusterLogic.forDDL().processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus());
        } else {
            // just release local lock
            ClusterLogic.forDDL().processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus());
        }


    }

    private void deleteNode(ClusterEvent<?> event, DDLInfo ddlInfo) throws Exception {
        ClusterLogic.forDDL().deleteDDLNodeEvent(ddlInfo, event.getPath());
    }


}
