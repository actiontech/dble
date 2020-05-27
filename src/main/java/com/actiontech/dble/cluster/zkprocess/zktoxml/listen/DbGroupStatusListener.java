package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Created by szf on 2019/10/30.
 */
public class DbGroupStatusListener implements PathChildrenCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupStatusListener.class);

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData childData = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED:
                break;
            case CHILD_UPDATED:
                updateStatus(childData);
                break;
            case CHILD_REMOVED:
                break;
            default:
                break;
        }
    }

    private void updateStatus(ChildData childData) {
        try {
            if (SystemConfig.getInstance().isUseOuterHa()) {
                String nodeName = childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
                String data = new String(childData.getData(), StandardCharsets.UTF_8);
                int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.RESPONSE_NOTIFY, HaInfo.HaStartType.CLUSTER_NOTIFY, "");
                PhysicalDbGroup physicalDBPool = DbleServer.getInstance().getConfig().getDbGroups().get(nodeName);
                physicalDBPool.changeIntoLatestStatus(data);
                HaConfigManager.getInstance().haFinish(id, null, data);
            }
        } catch (Exception e) {
            LOGGER.warn("get Error when update Ha status", e);
        }
    }

}
