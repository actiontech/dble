package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.singleton.ClusterGeneralConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import com.actiontech.dble.util.KVPathUtil;
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
public class DataHostResponseListener implements PathChildrenCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataHostResponseListener.class);

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
        if (DbleServer.getInstance().isUseOuterHa()) {
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
                ZKUtils.createTempNode(childData.getPath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), e.getMessage().getBytes());
            }
        }
    }


    private void response(String data, String path) throws Exception {
        HaInfo info = new HaInfo(data);
        CuratorFramework zkConn = ZKUtils.getConnection();
        if (!info.getStartId().equals(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID)) &&
                info.getStatus() == HaInfo.HaStatus.SUCCESS) {
            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.RESPONSE_NOTIFY, HaInfo.HaStartType.CLUSTER_NOTIFY, HaInfo.HaStage.RESPONSE_NOTIFY.toString());
            PhysicalDataHost dataHost = (PhysicalDataHost) DbleServer.getInstance().getConfig().getDataHosts().get(info.getDhName());
            String jsonString = new String(zkConn.getData().forPath(KVPathUtil.getHaStatusPath(info.getDhName())), "UTF-8");
            dataHost.changeIntoLatestStatus(jsonString);
            //response to kv
            ZKUtils.createTempNode(path, ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), ClusterPathUtil.SUCCESS.getBytes());
            HaConfigManager.getInstance().haFinish(id, null, data);
        }
    }

}
