/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.bean.ClusterAlertBean;
import com.actiontech.dble.config.model.SystemConfig;


public final class UcoreAlert implements Alert {
    private static final String SOURCE_COMPONENT_TYPE = "dble";
    private final String serverId;
    private final String sourceComponentId;
    private final String alertComponentId;
    private AbstractConsulSender sender;

    public UcoreAlert(AbstractConsulSender sender) {
        serverId = SystemConfig.getInstance().getServerId();
        sourceComponentId = SystemConfig.getInstance().getInstanceName();
        alertComponentId = SystemConfig.getInstance().getInstanceName();
        this.sender = sender;
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
        sender.alert(alert);
    }

    @Override
    public boolean alertResolve(ClusterAlertBean alert) {
        alert.setDesc("").
                setSourceComponentType(SOURCE_COMPONENT_TYPE).
                setSourceComponentId(sourceComponentId).
                setServerId(serverId).
                setResolveTimestampUnix(System.currentTimeMillis() * 1000000);
        return sender.alertResolve(alert);
    }

    @Override
    public boolean alertSelfResolve(ClusterAlertBean alert) {
        return alertResolve(alert.setAlertComponentType(SOURCE_COMPONENT_TYPE).setAlertComponentId(alertComponentId));
    }

    @Override
    public void alertConfigCheck() throws Exception {

    }

}
