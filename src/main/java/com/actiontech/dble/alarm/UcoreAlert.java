/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.bean.ClusterAlertBean;

import java.util.Map;

public final class UcoreAlert implements Alert {
    private static final String SOURCE_COMPONENT_TYPE = "dble";

    private static final UcoreAlert INSTANCE = new UcoreAlert();

    public static UcoreAlert getInstance() {
        return INSTANCE;
    }

    private UcoreAlert() {

    }

    @Override
    public void alertSelf(String code, AlertLevel level, String desc, Map<String, String> labels) {
        alert(code, level, desc, SOURCE_COMPONENT_TYPE, ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), labels);
    }

    @Override
    public void alert(String code, AlertLevel level, String desc, String alertComponentType, String alertComponentId, Map<String, String> labels) {
        ClusterAlertBean alert = new ClusterAlertBean();
        alert.setCode(code);
        alert.setDesc(desc);
        alert.setLevel(level.toString());
        alert.setSourceComponentType(SOURCE_COMPONENT_TYPE);
        alert.setSourceComponentId(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
        alert.setAlertComponentId(alertComponentId);
        alert.setAlertComponentType(alertComponentType);
        alert.setServerId(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_SERVER_ID));
        alert.setTimestampUnix(System.currentTimeMillis() * 1000000);
        if (labels != null) {
            alert.setLabels(labels);
        }
        ClusterHelper.alert(alert);
    }

    @Override
    public boolean alertResolve(String code, AlertLevel level, String alertComponentType, String alertComponentId, Map<String, String> labels) {
        ClusterAlertBean alert = new ClusterAlertBean();
        alert.setCode(code);
        alert.setDesc("");
        alert.setLevel(level.toString());
        alert.setSourceComponentType(SOURCE_COMPONENT_TYPE);
        alert.setSourceComponentId(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
        alert.setAlertComponentId(alertComponentId);
        alert.setAlertComponentType(alertComponentType);
        alert.setServerId(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_SERVER_ID));
        alert.setResolveTimestampUnix(System.currentTimeMillis() * 1000000);
        if (labels != null) {
            alert.setLabels(labels);
        }
        return ClusterHelper.alertResolve(alert);
    }

    @Override
    public boolean alertSelfResolve(String code, AlertLevel level, Map<String, String> labels) {
        return alertResolve(code, level, SOURCE_COMPONENT_TYPE, ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), labels);
    }

}
