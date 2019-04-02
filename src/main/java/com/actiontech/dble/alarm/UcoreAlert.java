/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.bean.ClusterAlertBean;


public final class UcoreAlert implements Alert {
    private static final String SOURCE_COMPONENT_TYPE = "dble";
    private final String serverId;
    private final String sourceComponentId;
    private final String alertComponentId;

    public UcoreAlert() {
        serverId = ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_SERVER_ID);
        sourceComponentId = ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
        alertComponentId = ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
    }

    @Override
    public void alertSelf(ClusterAlertBean alert) {
        alert(alert.setAlertComponentType(SOURCE_COMPONENT_TYPE).setAlertComponentId(alertComponentId));
    }

    @Override
    public void alert(ClusterAlertBean alert) {
        alert.setSourceComponentType(SOURCE_COMPONENT_TYPE).
                setSourceComponentId(sourceComponentId).
                setServerId(serverId).
                setTimestampUnix(System.currentTimeMillis() * 1000000);
        ClusterHelper.alert(alert);
    }

    @Override
    public boolean alertResolve(ClusterAlertBean alert) {
        alert.setDesc("").
                setSourceComponentType(SOURCE_COMPONENT_TYPE).
                setSourceComponentId(sourceComponentId).
                setServerId(serverId).
                setResolveTimestampUnix(System.currentTimeMillis() * 1000000);
        return ClusterHelper.alertResolve(alert);
    }

    @Override
    public boolean alertSelfResolve(ClusterAlertBean alert) {
        return alertResolve(alert.setAlertComponentType(SOURCE_COMPONENT_TYPE).setAlertComponentId(alertComponentId));
    }

}
