/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import com.actiontech.dble.cluster.general.bean.ClusterAlertBean;

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

    @Override
    public void alertConfigCheck() throws Exception {

    }
}
