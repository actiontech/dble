/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import com.actiontech.dble.cluster.general.bean.ClusterAlertBean;
import com.actiontech.dble.singleton.AlertManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class AlertUtil {
    private AlertUtil() {

    }

    private static volatile boolean isEnable = false;

    public static void switchAlert(boolean enableAlert) {
        isEnable = enableAlert;
    }

    public static boolean isEnable() {
        return isEnable;
    }

    public static void alertSelf(String code, Alert.AlertLevel level, String desc, Map<String, String> labels) {
        if (isEnable) {
            ClusterAlertBean bean = new ClusterAlertBean();
            bean.setCode(code).setLevel(level.toString()).setDesc(desc).setLabels(labels);
            AlertTask task = new AlertTask(Alert.AlertType.ALERT_SELF, null, null, bean);
            AlertManager.getInstance().getAlertQueue().offer(task);
        }
    }

    public static void alert(String code, Alert.AlertLevel level, String desc, String alertComponentType, String alertComponentId, Map<String, String> labels) {
        if (isEnable) {
            ClusterAlertBean bean = new ClusterAlertBean();
            bean.setCode(code).setLevel(level.toString()).setDesc(desc).setLabels(labels).setAlertComponentType(alertComponentType).setAlertComponentId(alertComponentId);
            AlertTask task = new AlertTask(Alert.AlertType.ALERT, null, null, bean);
            AlertManager.getInstance().getAlertQueue().offer(task);
        }
    }

    public static void alertResolve(String code, Alert.AlertLevel level, String alertComponentType, String alertComponentId, Map<String, String> labels, Set<String> callbackSet, String callbackKey) {
        if (isEnable) {
            ClusterAlertBean bean = new ClusterAlertBean();
            bean.setCode(code).setLevel(level.toString()).setAlertComponentId(alertComponentId).setAlertComponentType(alertComponentType).setLabels(labels);
            AlertTask task = new AlertTask(Alert.AlertType.ALERT_RESOLVE, callbackSet, callbackKey, bean);
            AlertManager.getInstance().getAlertQueue().offer(task);
        }
    }

    public static void alertResolve(String code, Alert.AlertLevel level, String alertComponentType, String alertComponentId, Map<String, String> labels) {
        if (isEnable) {
            ClusterAlertBean bean = new ClusterAlertBean();
            bean.setCode(code).setLevel(level.toString()).setAlertComponentId(alertComponentId).setAlertComponentType(alertComponentType).setLabels(labels);
            AlertTask task = new AlertTask(Alert.AlertType.ALERT_RESOLVE, null, null, bean);
            AlertManager.getInstance().getAlertQueue().offer(task);
        }
    }


    public static void alertSelfResolve(String code, Alert.AlertLevel level, Map<String, String> labels, Set<String> callbackSet, String callbackKey) {
        if (isEnable) {
            ClusterAlertBean bean = new ClusterAlertBean();
            bean.setCode(code).setLevel(level.toString()).setLabels(labels);
            AlertTask task = new AlertTask(Alert.AlertType.ALERT_SELF_RESOLVE, callbackSet, callbackKey, bean);
            AlertManager.getInstance().getAlertQueue().offer(task);
        }
    }

    public static void alertSelfResolve(String code, Alert.AlertLevel level, Map<String, String> labels) {
        if (isEnable) {
            ClusterAlertBean bean = new ClusterAlertBean();
            bean.setCode(code).setLevel(level.toString()).setLabels(labels);
            AlertTask task = new AlertTask(Alert.AlertType.ALERT_SELF_RESOLVE, null, null, bean);
            AlertManager.getInstance().getAlertQueue().offer(task);
        }
    }


    public static Map<String, String> genSingleLabel(String key, String value) {
        Map<String, String> labels = new HashMap<>(1);
        labels.put(key, value);
        return labels;
    }
}
