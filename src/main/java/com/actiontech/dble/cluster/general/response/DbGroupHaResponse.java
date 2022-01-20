/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.path.ChildPathMeta;
import com.actiontech.dble.cluster.path.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.*;

import static com.actiontech.dble.cluster.path.ClusterPathUtil.DB_GROUP_STATUS;

/**
 * Created by szf on 2019/10/29.
 */
public class DbGroupHaResponse extends AbstractGeneralListener<Empty> {
    public static final ChildPathMeta<Empty> HA_BASE_PATH = ClusterChildMetaUtil.getHaBasePath();

    public DbGroupHaResponse() {
        super(HA_BASE_PATH);
    }

    @Override
    public void onEvent(ClusterEvent<Empty> configValue) throws Exception {
        if (!DbleServer.getInstance().isStartup()) {
            return;
        }
        String path = configValue.getPath();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        if (paths.length != ClusterLogic.forHA().getPathHeight(HA_BASE_PATH.getPath()) + 2) {
            //child change the listener is not supported
            return;
        }
        if (configValue.getChangeType().equals(ChangeType.REMOVED)) {
            return;
        }

        String dbGroupName = paths[paths.length - 1];
        if (path.contains(DB_GROUP_STATUS)) {
            //ha status ,using rawJson
            ClusterLogic.forHA().dbGroupChangeEvent(dbGroupName, configValue.getValue().convertTo(RawJson.class).getData());
        } else {
            //ha response,using HaInfo
            ClusterLogic.forHA().dbGroupResponseEvent(configValue.getValue().convertTo(HaInfo.class).getData(), dbGroupName);
        }
    }


    @Override
    public void notifyCluster() throws Exception {
        ClusterDelayProvider.delayBeforeUploadHa();
        ClusterLogic.forHA().syncDbGroupStatusToCluster();
    }
}
