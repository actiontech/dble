/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import com.actiontech.dble.cluster.bean.ClusterAlertBean;

public class NoAlert implements Alert {


    @Override
    public void alertSelf(ClusterAlertBean bean) {

    }

    @Override
    public void alert(ClusterAlertBean bean) {

    }

    @Override
    public boolean alertResolve(ClusterAlertBean bean) {
        return true;
    }

    @Override
    public boolean alertSelfResolve(ClusterAlertBean bean) {
        return true;
    }
}
