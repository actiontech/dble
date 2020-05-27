package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.cluster.ClusterPathUtil.DB_GROUP_STATUS;

/**
 * Created by szf on 2019/10/29.
 */
public class DbGroupHaResponse implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(DdlChildResponse.class);

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        if (configValue.getKey().split("/").length == ClusterPathUtil.getHaStatusPath().split("/").length + 2) {
            //child change the listener is not supported
            return;
        } else if (configValue.getChangeType().equals(KvBean.DELETE)) {
            return;
        }
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        if (configValue.getKey().contains(DB_GROUP_STATUS)) {
            KvBean reloadStatus = ClusterHelper.getKV(ClusterPathUtil.getConfStatusPath());
            int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.RESPONSE_NOTIFY, HaInfo.HaStartType.CLUSTER_NOTIFY, "");
            while (reloadStatus != null) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                reloadStatus = ClusterHelper.getKV(ClusterPathUtil.getConfStatusPath());
            }
            String[] path = configValue.getKey().split("/");
            String dhName = path[path.length - 1];
            PhysicalDbGroup dataHost = DbleServer.getInstance().getConfig().getDbGroups().get(dhName);
            dataHost.changeIntoLatestStatus(configValue.getValue());
            HaConfigManager.getInstance().haFinish(id, null, configValue.getValue());
        } else {
            //data_host_locks events,we only try to response to the DISABLE,ignore others
            HaInfo info = new HaInfo(configValue.getValue());
            if (info.getLockType() == HaInfo.HaType.DATAHOST_DISABLE &&
                    !info.getStartId().equals(SystemConfig.getInstance().getInstanceId()) &&
                    info.getStatus() == HaInfo.HaStatus.SUCCESS) {
                try {
                    //start the log
                    int id = HaConfigManager.getInstance().haStart(HaInfo.HaStage.RESPONSE_NOTIFY, HaInfo.HaStartType.CLUSTER_NOTIFY, HaInfo.HaStage.RESPONSE_NOTIFY.toString());
                    //try to get the lastest status of the dbGroup
                    KvBean latestStatus = ClusterHelper.getKV(ClusterPathUtil.getHaStatusPath(info.getDbGroupName()));
                    //find out the target dbGroup and change it into latest status
                    PhysicalDbGroup dataHost = DbleServer.getInstance().getConfig().getDbGroups().get(info.getDbGroupName());
                    dataHost.changeIntoLatestStatus(latestStatus.getValue());
                    //response the event ,only disable event has response
                    ClusterHelper.setKV(ClusterPathUtil.getSelfResponsePath(configValue.getKey()), ClusterPathUtil.SUCCESS);
                    //ha manager writeOut finish log
                    HaConfigManager.getInstance().haFinish(id, null, latestStatus.getValue());
                } catch (Exception e) {
                    //response the event ,only disable event has response
                    ClusterHelper.setKV(ClusterPathUtil.getSelfResponsePath(configValue.getKey()), e.getMessage());
                }
            }
        }
    }

    @Override
    public void notifyCluster() throws Exception {
        HaConfigManager.getInstance().init();
        if (ClusterConfig.getInstance().isNeedSyncHa()) {
            Map<String, String> map = HaConfigManager.getInstance().getSourceJsonList();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(entry.getKey()), entry.getValue());
            }
        }
    }
}
