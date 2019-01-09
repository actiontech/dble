/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;

import java.util.Map;

public final class UcoreAlert implements Alert {
    private static final String SOURCE_COMPONENT_TYPE = "dble";
    private static final String SERVER_ID = UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_SERVER_ID);
    private static final String SOURCE_COMPONENT_ID = UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);

    private static final UcoreAlert INSTANCE = new UcoreAlert();

    public static UcoreAlert getInstance() {
        return INSTANCE;
    }

    private UcoreAlert() {

    }

    @Override
    public void alertSelf(String code, AlertLevel level, String desc, Map<String, String> labels) {
        alert(code, level, desc, SOURCE_COMPONENT_TYPE, SOURCE_COMPONENT_ID, labels);
    }

    @Override
    public void alert(String code, AlertLevel level, String desc, String alertComponentType, String alertComponentId, Map<String, String> labels) {
        UcoreInterface.AlertInput.Builder builder = UcoreInterface.AlertInput.newBuilder().
                setCode(code).
                setDesc(desc).
                setLevel(level.toString()).
                setSourceComponentType(SOURCE_COMPONENT_TYPE).
                setSourceComponentId(SOURCE_COMPONENT_ID).
                setAlertComponentId(alertComponentId).
                setAlertComponentType(alertComponentType).
                setServerId(SERVER_ID).
                setTimestampUnix(System.currentTimeMillis() * 1000000);
        if (labels != null) {
            builder.putAllLabels(labels);
        }
        UcoreInterface.AlertInput input = builder.build();
        ClusterUcoreSender.alert(input);
    }

    @Override
    public boolean alertResolve(String code, AlertLevel level, String alertComponentType, String alertComponentId, Map<String, String> labels) {
        UcoreInterface.AlertInput.Builder builder = UcoreInterface.AlertInput.newBuilder().
                setCode(code).
                setDesc("").
                setLevel(level.toString()).
                setSourceComponentType(SOURCE_COMPONENT_TYPE).
                setSourceComponentId(SOURCE_COMPONENT_ID).
                setAlertComponentId(alertComponentId).
                setAlertComponentType(alertComponentType).
                setServerId(SERVER_ID).
                setResolveTimestampUnix(System.currentTimeMillis() * 1000000);
        if (labels != null) {
            builder.putAllLabels(labels);
        }
        UcoreInterface.AlertInput input = builder.build();
        return ClusterUcoreSender.alertResolve(input);
    }

    @Override
    public boolean alertSelfResolve(String code, AlertLevel level, Map<String, String> labels) {
        return alertResolve(code, level, SOURCE_COMPONENT_TYPE, SOURCE_COMPONENT_ID, labels);
    }

}
