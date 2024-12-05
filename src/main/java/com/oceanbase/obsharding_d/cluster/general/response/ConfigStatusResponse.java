/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.btrace.provider.ClusterDelayProvider;
import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ChildPathMeta;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.ChangeType;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.ConfStatus;
import com.oceanbase.obsharding_d.config.model.SystemConfig;

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
        if (!OBsharding_DServer.getInstance().isStartup()) {
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
