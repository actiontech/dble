/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/1/31.
 */
public class ConfigStatusResponse implements ClusterXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusResponse.class);


    public ConfigStatusResponse() {
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        if (!DbleServer.getInstance().isStartup()) {
            return;
        }

        ClusterDelayProvider.delayAfterGetNotice();
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        String path = configValue.getKey();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        if (paths.length != ClusterLogic.getPathHeight(ClusterPathUtil.getConfStatusPath()) + 1) {
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
        String value = configValue.getValue();

        //step 1 check if the change is from itself
        ConfStatus status = new ConfStatus(value);
        if (status.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            //self node
            return;
        }
        //step 2reload the config and set the self config status
        ClusterLogic.reloadConfigEvent(value, status.getParams());
    }



    @Override
    public void notifyCluster() throws Exception {

    }
}
