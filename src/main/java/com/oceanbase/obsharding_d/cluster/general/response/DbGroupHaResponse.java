/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.btrace.provider.ClusterDelayProvider;
import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ChildPathMeta;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.*;

import static com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil.DB_GROUP_STATUS;

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
        if (!OBsharding_DServer.getInstance().isStartup()) {
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
