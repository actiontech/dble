/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.logic.ClusterOperation;
import com.actiontech.dble.cluster.path.ClusterMetaUtil;
import com.actiontech.dble.cluster.values.ClusterValue;
import com.actiontech.dble.cluster.values.RawJson;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceToPropertiesListener implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(SequenceToPropertiesListener.class);


    public SequenceToPropertiesListener(ZookeeperProcessListen zookeeperListen) {
        zookeeperListen.addToInit(this);
    }

    @Override
    public void notifyProcess() throws Exception {
        final String path = ClusterMetaUtil.getSequencesCommonPath().getPath();
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.CONFIG);
        final RawJson configValue = clusterHelper.getPathValue(ClusterMetaUtil.getSequencesCommonPath()).map(ClusterValue::getData).orElse(null);
        if (configValue == null) {
            LOGGER.warn("receive empty value");
        }
        LOGGER.info("notify " + path + " " + configValue);
        ClusterLogic.forConfig().syncSequenceJson(path, configValue);
    }
}
