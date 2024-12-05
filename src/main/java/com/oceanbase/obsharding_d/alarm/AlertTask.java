/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.alarm;

import com.oceanbase.obsharding_d.cluster.general.bean.ClusterAlertBean;

import java.util.Set;

/**
 * Created by szf on 2019/3/29.
 */
public class AlertTask {

    private ClusterAlertBean alertBean;
    private Alert.AlertType alertType;
    private Set<String> callbackSet;
    private String callbackKey;


    public AlertTask(Alert.AlertType alertType, Set<String> callbackSet, String callbackKey, ClusterAlertBean alertBean) {
        this.alertBean = alertBean;
        this.alertType = alertType;
        this.callbackSet = callbackSet;
        this.callbackKey = callbackKey;

    }


    public ClusterAlertBean getAlertBean() {
        return alertBean;
    }

    public Alert.AlertType getAlertType() {
        return alertType;
    }


    public void alertCallBack() {
        if (callbackSet != null &&
                callbackKey != null) {
            callbackSet.remove(callbackKey);
        }
    }

}
