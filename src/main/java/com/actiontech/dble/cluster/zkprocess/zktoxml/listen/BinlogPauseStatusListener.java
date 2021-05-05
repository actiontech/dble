/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.ClusterEvent;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.values.Empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huqing.yan on 2017/5/25.
 */
public class BinlogPauseStatusListener extends AbstractGeneralListener<Empty> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusListener.class);

    public BinlogPauseStatusListener() {
        super(ClusterChildMetaUtil.getBinlogPausePath());
    }

    @Override
    public void onEvent(ClusterEvent<Empty> event) throws Exception {
        switch (event.getChangeType()) {
            case ADDED: {
                LOGGER.info("childEvent " + event.getPath() + " " + event.getChangeType());
                String instanceName = event.getValue().getInstanceName();
                ClusterLogic.forBinlog().executeBinlogPauseEvent(instanceName);
            }
            break;
            case REMOVED: {
                LOGGER.info("childEvent " + event.getPath() + " " + event.getChangeType());
                String instanceName = event.getValue().getInstanceName();
                ClusterLogic.forBinlog().executeBinlogPauseDeleteEvent(instanceName);
            }
            break;
            default:
                break;
        }
    }


}
