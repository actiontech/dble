/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.*;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.values.Empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/1/31.
 */
public class BinlogPauseStatusResponse extends AbstractGeneralListener<Empty> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusResponse.class);
    public static final ChildPathMeta<Empty> BINLOG_PAUSE_PATH = ClusterChildMetaUtil.getBinlogPausePath();

    public BinlogPauseStatusResponse() {
        super(BINLOG_PAUSE_PATH);
    }

    @Override
    public void onEvent(ClusterEvent<Empty> configValue) throws Exception {
        if (!DbleServer.getInstance().isStartup()) {
            return;
        }

        String path = configValue.getPath();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        if (paths.length != ClusterLogic.forBinlog().getPathHeight(BINLOG_PAUSE_PATH.getPath()) + 1) {
            return;
        }

        String instanceName = configValue.getValue().getInstanceName();
        if (ChangeType.REMOVED.equals(configValue.getChangeType())) {
            ClusterLogic.forBinlog().executeBinlogPauseDeleteEvent(instanceName);
        } else {
            ClusterLogic.forBinlog().executeBinlogPauseEvent(instanceName);
        }
    }


    @Override
    public void notifyCluster() throws Exception {
    }
}
