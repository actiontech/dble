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


        switch (configValue.getChangeType()) {
            case KvBean.ADD: {
                String key = paths[paths.length - 1];
                initMeta(key, ddlInfo);
                break;
            }
            case KvBean.UPDATE: {
                String key = paths[paths.length - 1];
                updateMeta(key, ddlInfo);
                break;
            }
            case KvBean.DELETE:
                ClusterLogic.deleteDDLNodeEvent(ddlInfo, path);
                break;
            default:
                break;
        }


    }


    @Override
    public void notifyCluster() throws Exception {
    }


    private void initMeta(String keyName, DDLInfo ddlInfo) {


        ClusterLogic.processStatusEvent(keyName, ddlInfo, DDLInfo.DDLStatus.INIT);

        if (DDLInfo.DDLStatus.INIT != ddlInfo.getStatus()) {
            LOGGER.warn("get a special CREATE event when doing cluster ddl , status:{}, data is {}", ddlInfo.getStatus(), ddlInfo.toString());
            ClusterLogic.processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus());
        }


    }


    private void updateMeta(String keyName, DDLInfo ddlInfo) {


        if (DDLInfo.DDLStatus.INIT == ddlInfo.getStatus()) {
            //missing DELETE event.
            LOGGER.warn("get a special UPDATE event when doing cluster ddl , status:{}, data is {}", ddlInfo.getStatus(), ddlInfo.toString());
            ClusterLogic.processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus());
        } else {
            // just release local lock
            ClusterLogic.processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus());
        }


    }


}
