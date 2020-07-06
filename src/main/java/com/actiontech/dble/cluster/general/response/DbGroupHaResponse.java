package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.actiontech.dble.cluster.ClusterPathUtil.DB_GROUP_STATUS;

/**
 * Created by szf on 2019/10/29.
 */
public class DbGroupHaResponse implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(DdlChildResponse.class);

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        if (!DbleServer.getInstance().isStartup()) {
            return;
        }
        String path = configValue.getKey();
        String value = configValue.getValue();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        if (paths.length != ClusterLogic.getPathHeight(ClusterPathUtil.getHaBasePath()) + 2) {
            //child change the listener is not supported
            return;
        }
        if (configValue.getChangeType().equals(KvBean.DELETE)) {
            return;
        }

        String dbGroupName = paths[paths.length - 1];
        LOGGER.info("notify " + path + " " + value + " " + configValue.getChangeType());
        if (path.contains(DB_GROUP_STATUS)) {
            ClusterLogic.dbGroupChangeEvent(dbGroupName, value);
        } else {
            ClusterLogic.dbGroupResponseEvent(value, dbGroupName);
        }
    }


    @Override
    public void notifyCluster() throws Exception {
        ClusterLogic.syncDbGroupStatusToCluster();
    }
}
