/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen;

import com.oceanbase.obsharding_d.btrace.provider.ClusterDelayProvider;
import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.ClusterValue;
import com.oceanbase.obsharding_d.cluster.values.DDLInfo;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
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


        switch (event.getChangeType()) {
            case ADDED:
                processDDL(event, ddlInfo);
                break;
            case REMOVED:
                deleteNode(event, ddlInfo);
                break;
            default:
                break;
        }
    }

    private void processDDL(ClusterEvent<?> event, DDLInfo ddlInfo) {

        final String childPath = event.getPath();
        String keyName = childPath.substring(childPath.lastIndexOf("/") + 1);

        ClusterLogic.forDDL().processStatusEvent(keyName, ddlInfo, ddlInfo.getStatus(), childPath);

    }

    private void deleteNode(ClusterEvent<?> event, DDLInfo ddlInfo) throws Exception {
        ClusterLogic.forDDL().deleteDDLNodeEvent(ddlInfo, event.getPath());
    }


}
