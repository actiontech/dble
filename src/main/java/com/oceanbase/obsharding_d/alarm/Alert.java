/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.alarm;

import com.oceanbase.obsharding_d.cluster.general.bean.ClusterAlertBean;


public interface Alert {
    enum AlertLevel {
        NOTICE, WARN, CRITICAL
    }

    enum AlertType {
        ALERT, ALERT_RESOLVE, ALERT_SELF, ALERT_SELF_RESOLVE
    }

    void alertSelf(ClusterAlertBean bean);

    void alert(ClusterAlertBean bean);

    boolean alertResolve(ClusterAlertBean bean);

    boolean alertSelfResolve(ClusterAlertBean bean);

    void alertConfigCheck() throws Exception;
}
