/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.path.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.ClusterEvent;
import com.actiontech.dble.cluster.values.DDLInfo;
import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/2/1.
 */
public class DdlChildResponse extends AbstractGeneralListener<DDLInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlChildResponse.class);

    public DdlChildResponse() {
        super(ClusterChildMetaUtil.getDDLPath());
    }

    @Override
    public void onEvent(ClusterEvent<DDLInfo> configValue) throws Exception {

        String path = configValue.getPath();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        if (paths.length != ClusterLogic.forDDL().getPathHeight(ClusterPathUtil.getDDLPath()) + 1) {
            //child change the listener is not supported
            //only response for the key /un.../d.../clu.../ddl/sharding.table
            return;
        }

        ClusterDelayProvider.delayAfterGetDdlNotice();
        DDLInfo ddlInfo = configValue.getValue().getData();


        if (ddlInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            LOGGER.info("DDL node " + path + " is from myself ,so just return ,and data is " + ddlInfo.toString());
            return; //self node
        }


        switch (configValue.getChangeType()) {
            case ADDED: {
                String key = paths[paths.length - 1];
                processDDL(key, ddlInfo, path);
                break;
            }
            case REMOVED:
                ClusterLogic.forDDL().deleteDDLNodeEvent(ddlInfo, path);
                break;
            default:
                break;
        }

    }

    @Override
    public void notifyCluster() throws Exception {
    }

    private void processDDL(String keyName, DDLInfo ddlInfo, String path) {
        ClusterLogic.forDDL().processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus(), path);
    }

}
