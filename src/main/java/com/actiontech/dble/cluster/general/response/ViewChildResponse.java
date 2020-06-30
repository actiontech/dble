/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/2/5.
 */
public class ViewChildResponse implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewChildResponse.class);

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        String path = configValue.getKey();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        if (paths.length != ClusterLogic.getPathHeight(ClusterPathUtil.getViewChangePath()) + 1) {
            //only with the type u.../d.../clu.../view/update(delete)/sharding.table
            return;
        }
        if ("".equals(configValue.getValue())) {
            //the value of key is empty,just doing nothing
            return;
        }
        if (KvBean.DELETE.equals(configValue.getChangeType())) {
            // delete node
            return;
        }
        String key = paths[paths.length - 1];
        String value = configValue.getValue();
        ClusterLogic.executeViewEvent(path, key, value);
    }


    @Override
    public void notifyCluster() throws Exception {
    }
}
