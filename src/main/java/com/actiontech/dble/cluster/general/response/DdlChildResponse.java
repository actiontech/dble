/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.values.DDLInfo;
import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/2/1.
 */
public class DdlChildResponse implements ClusterXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlChildResponse.class);

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        String path = configValue.getKey();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        if (paths.length != ClusterLogic.getPathHeight(ClusterPathUtil.getDDLPath()) + 1) {
            //child change the listener is not supported
            //only response for the key /un.../d.../clu.../ddl/sharding.table
            return;
        }

        ClusterDelayProvider.delayAfterGetDdlNotice();
        String value = configValue.getValue();
        if ("".equals(value)) {
            return;
        }


        DDLInfo ddlInfo = new DDLInfo(value);
        if (ddlInfo.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            LOGGER.info("DDL node " + path + " is from myself ,so just return ,and data is " + ddlInfo.toString());
            return; //self node
        }

        if (KvBean.DELETE.equals(configValue.getChangeType())) {
            ClusterLogic.deleteDDLNodeEvent(ddlInfo, path);
            return;
        }

        String key = paths[paths.length - 1];
        //if the start node is preparing to do the ddl
        if (ddlInfo.getStatus() == DDLInfo.DDLStatus.INIT) {
            ClusterLogic.initDDLEvent(key, ddlInfo);
        } else if (ddlInfo.getStatus() == DDLInfo.DDLStatus.SUCCESS) {

            ClusterLogic.ddlUpdateEvent(key, ddlInfo);
        } else if (ddlInfo.getStatus() == DDLInfo.DDLStatus.FAILED) {
            ClusterLogic.ddlFailedEvent(key);
        }
    }


    @Override
    public void notifyCluster() throws Exception {
    }

}
