/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;

import java.util.HashMap;
import java.util.Map;

public final class AlertUtil {
    private AlertUtil() {

    }

    private static volatile Alert alert;
    private static final Alert DEFAULT_ALERT = new NoAlert();
    private static volatile boolean isEnable = false;

    static {
        alert = DEFAULT_ALERT;
    }

    public static void switchAlert(boolean enableAlert) {
        isEnable = enableAlert;
        if (enableAlert && UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID) != null) {
            alert = UcoreAlert.getInstance();
        } else {
            alert = DEFAULT_ALERT;
        }
    }

    public static boolean isEnable() {
        return isEnable;
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

    public static Map<String, String> genSingleLabel(String key, String value) {
        Map<String, String> labels = new HashMap<>(1);
        labels.put(key, value);
        return labels;
    }
}
