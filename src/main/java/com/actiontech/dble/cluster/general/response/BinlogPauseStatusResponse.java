/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/1/31.
 */
public class BinlogPauseStatusResponse implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusResponse.class);


    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        if (!DbleServer.getInstance().isStartup()) {
            return;
        }
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        String path = configValue.getKey();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        if (paths.length != ClusterLogic.getPathHeight(ClusterPathUtil.getBinlogPause()) + 1) {
            return;
        }
        if ("".equals(configValue.getValue())) {
            //the value of key is empty,just doing nothing
            return;
        }

        String value = configValue.getValue();
        if (KvBean.DELETE.equals(configValue.getChangeType())) {
            ClusterLogic.executeBinlogPauseDeleteEvent(value);
        } else {
            ClusterLogic.executeBinlogPauseEvent(value);
        }
    }


    @Override
    public void notifyCluster() throws Exception {
    }
}
