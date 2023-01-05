/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.path.ChildPathMeta;
import com.actiontech.dble.cluster.path.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.ChangeType;
import com.actiontech.dble.cluster.values.ClusterEvent;
import com.actiontech.dble.cluster.values.Empty;

/**
 * Created by szf on 2018/1/31.
 */
public class BinlogPauseStatusResponse extends AbstractGeneralListener<Empty> {

    private static final ChildPathMeta<Empty> BINLOG_PAUSE_PATH = ClusterChildMetaUtil.getBinlogPausePath();

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

        if (ChangeType.REMOVED.equals(configValue.getChangeType())) {
            ClusterLogic.forBinlog().executeBinlogPauseDeleteEvent(configValue);
        } else {
            ClusterLogic.forBinlog().executeBinlogPauseEvent(configValue);
        }
    }


    @Override
    public void notifyCluster() throws Exception {
    }
}
