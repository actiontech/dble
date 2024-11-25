/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ChildPathMeta;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.ChangeType;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.Empty;

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
        if (!OBsharding_DServer.getInstance().isStartup()) {
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
