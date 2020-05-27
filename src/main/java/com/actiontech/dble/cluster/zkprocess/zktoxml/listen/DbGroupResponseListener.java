package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.util.ZKUtils;
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
public class DbGroupResponseListener implements PathChildrenCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupResponseListener.class);

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData childData = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED:
                updateStatus(childData);
                break;
            case CHILD_UPDATED:
                break;
            case CHILD_REMOVED:
                break;
            default:
                break;
        }
    }


    private void updateStatus(ChildData childData) throws Exception {
        if (SystemConfig.getInstance().isUseOuterHa()) {
            String data = new String(childData.getData(), StandardCharsets.UTF_8);
            LOGGER.info("Ha disable node " + childData.getPath() + " updated , and data is " + data);
            try {
                if (!"".equals(data)) {
                    response(data, childData.getPath());
                } else {
                    CuratorFramework zkConn = ZKUtils.getConnection();
                    String newData = new String(zkConn.getData().forPath(childData.getPath()), "UTF-8");
                    response(newData, childData.getPath());
                }
            } catch (Exception e) {
                LOGGER.warn("get error when try to response to the disable");
                ZKUtils.createTempNode(childData.getPath(), SystemConfig.getInstance().getInstanceName(), e.getMessage().getBytes());
            }
        }
    }


    private void response(String data, String path) throws Exception {
        HaInfo info = new HaInfo(data);
        CuratorFramework zkConn = ZKUtils.getConnection();
        if (!info.getStartId().equals(SystemConfig.getInstance().getInstanceName()) &&
                info.getStatus() == HaInfo.HaStatus.SUCCESS) {
            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.RESPONSE_NOTIFY, HaInfo.HaStartType.CLUSTER_NOTIFY, HaInfo.HaStage.RESPONSE_NOTIFY.toString());
            PhysicalDbGroup dataHost = (PhysicalDbGroup) DbleServer.getInstance().getConfig().getDbGroups().get(info.getDbGroupName());
            String jsonString = new String(zkConn.getData().forPath(ClusterPathUtil.getHaStatusPath(info.getDbGroupName())), "UTF-8");
            dataHost.changeIntoLatestStatus(jsonString);
            //response to kv
            ZKUtils.createTempNode(path, SystemConfig.getInstance().getInstanceName(), ClusterPathUtil.SUCCESS.getBytes());
            HaConfigManager.getInstance().haFinish(id, null, data);
        }
    }

}
