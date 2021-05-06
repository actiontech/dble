package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.*;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.values.Empty;
import com.actiontech.dble.cluster.values.HaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.actiontech.dble.cluster.ClusterPathUtil.DB_GROUP_STATUS;

/**
 * Created by szf on 2019/10/29.
 */
public class DbGroupHaResponse extends AbstractGeneralListener<Empty> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DdlChildResponse.class);
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
        ClusterLogic.forHA().syncDbGroupStatusToCluster();
    }
}
