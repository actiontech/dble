/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.response;

import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.ChangeType;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.ViewChangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/2/5.
 */
public class ViewChildResponse extends AbstractGeneralListener<ViewChangeType> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewChildResponse.class);

    public ViewChildResponse() {
        super(ClusterChildMetaUtil.getViewChangePath());
    }

    @Override
    public void onEvent(ClusterEvent<ViewChangeType> configValue) throws Exception {
        String path = configValue.getPath();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        if (paths.length != ClusterLogic.forView().getPathHeight(ClusterPathUtil.getViewChangePath()) + 1) {
            //only with the type u.../d.../clu.../view/update(delete)/sharding.table
            return;
        }

        if (ChangeType.REMOVED.equals(configValue.getChangeType())) {
            // delete node
            return;
        }
        String key = paths[paths.length - 1];
        final ViewChangeType data = configValue.getValue().getData();
        ClusterLogic.forView().executeViewEvent(path, key, data);
    }

    @Override
    public void notifyCluster() throws Exception {
    }
}
