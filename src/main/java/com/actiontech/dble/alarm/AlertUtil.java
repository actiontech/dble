/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;

import java.util.Map;

public final class AlertUtil {
    private AlertUtil() {

    }

    private static Alert alert;

    static {
        if (UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID) != null) {
            alert = UcoreAlert.getInstance();
        } else {
            alert = new NoAlert();
        }
    }

    public static void alertSelfWithTarget(String code, Alert.AlertLevel level, String desc, String alertComponentId, Map<String, String> labels) {
        alert.alertSelfWithTarget(code, level, desc, alertComponentId, labels);
    }

    public static void alertSelf(String code, Alert.AlertLevel level, String desc, Map<String, String> labels) {
        alert.alertSelf(code, level, desc, labels);
    }

    public static void alert(String code, Alert.AlertLevel level, String desc, String alertComponentType, String alertComponentId, Map<String, String> labels) {
        alert.alert(code, level, desc, alertComponentType, alertComponentId, labels);
    }

    public static boolean alertResolve(String code, Alert.AlertLevel level, String alertComponentType, String alertComponentId, Map<String, String> labels) {
        return alert.alertResolve(code, level, alertComponentType, alertComponentId, labels);
    }

    public static boolean alertSelfResolve(String code, Alert.AlertLevel level, Map<String, String> labels) {
        return alert.alertSelfResolve(code, level, labels);
    }
    public static boolean alertSelfWithTargetResolve(String code, Alert.AlertLevel level, String alertComponentId, Map<String, String> labels) {
        return alert.alertSelfWithTargetResolve(code, level, alertComponentId, labels);
    }
}
