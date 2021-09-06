/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.path.ChildPathMeta;
import com.actiontech.dble.cluster.path.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.ChangeType;
import com.actiontech.dble.cluster.values.ClusterEvent;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.config.model.SystemConfig;

/**
 * Created by szf on 2018/1/31.
 */
public class ConfigStatusResponse extends AbstractGeneralListener<ConfStatus> {

    private static final ChildPathMeta<ConfStatus> CONFIG_STATUS_OPERATOR_PATH = ClusterChildMetaUtil.getConfStatusOperatorPath();

    public ConfigStatusResponse() {
        super(CONFIG_STATUS_OPERATOR_PATH);
    }


    @Override
    public void onEvent(ClusterEvent<ConfStatus> configValue) throws Exception {
        if (!DbleServer.getInstance().isStartup()) {
            return;
        }

        ClusterDelayProvider.delayAfterGetNotice();

        String path = configValue.getPath();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        if (paths.length != ClusterLogic.forConfig().getPathHeight(CONFIG_STATUS_OPERATOR_PATH.getPath())) {
            return;
        }
        ConfStatus status = configValue.getValue().getData();

        if (ChangeType.REMOVED.equals(configValue.getChangeType())) {
            // delete node
            return;
        }

        //step 1 check if the change is from itself
        if (status.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            //self node
            return;
        }
        //step 2reload the config and set the self config status
        ClusterLogic.forConfig().reloadConfigEvent(status, status.getParams());
    }


    @Override
    public void notifyCluster() throws Exception {

    }
}
