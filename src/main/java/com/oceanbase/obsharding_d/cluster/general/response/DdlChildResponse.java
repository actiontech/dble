/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.response;

import com.oceanbase.obsharding_d.btrace.provider.ClusterDelayProvider;
import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.DDLInfo;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
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
