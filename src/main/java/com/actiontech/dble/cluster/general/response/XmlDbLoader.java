/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/1/26.
 */
public class XmlDbLoader implements ClusterXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(XmlDbLoader.class);


    public XmlDbLoader(ClusterClearKeyListener confListener) {
        confListener.addChild(this, ClusterPathUtil.getDbConfPath());
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        ClusterLogic.syncDbJson(configValue);
    }

    @Override
    public void notifyCluster() throws Exception {
        ClusterLogic.syncDbXmlToCluster();
    }

}
