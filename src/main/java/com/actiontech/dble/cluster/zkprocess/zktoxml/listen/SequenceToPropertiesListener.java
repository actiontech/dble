/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
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
    public boolean notifyProcess() throws Exception {
        KvBean configValue = ClusterHelper.getKV(ClusterPathUtil.getSequencesCommonPath());
        if (configValue == null) {
            throw new RuntimeException(ClusterPathUtil.getSequencesCommonPath() + " is null");
        }
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue());
        ClusterLogic.syncSequenceToLocal(configValue);
        return true;
    }
}
