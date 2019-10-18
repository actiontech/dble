package com.actiontech.dble.cluster.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.HaInfo;
import com.actiontech.dble.singleton.ClusterGeneralConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.cluster.ClusterPathUtil.DATA_HOST_STATUS;

/**
 * Created by szf on 2019/10/29.
 */
public class DataHostHaResponse implements ClusterXmlLoader {
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
        if (configValue.getKey().contains(DATA_HOST_STATUS)) {
            KvBean reloadStatus = ClusterHelper.getKV(ClusterPathUtil.getConfStatusPath());
            while (reloadStatus != null) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                reloadStatus = ClusterHelper.getKV(ClusterPathUtil.getConfStatusPath());
                continue;
            }
            //try to change dataHost status just like the
            String[] path = configValue.getKey().split("/");
            String dhName = path[path.length - 1];
            PhysicalDNPoolSingleWH dataHost = (PhysicalDNPoolSingleWH) DbleServer.getInstance().getConfig().getDataHosts().get(dhName);
            dataHost.changeIntoLastestStatus(configValue.getValue());
        } else {
            //data_host_locks events,we only try to response to the DISABLE,ignore others
            HaInfo info = new HaInfo(configValue.getValue());
            if (info.getLockType() == HaInfo.HaType.DATAHOST_DISABLE &&
                    !info.getStartId().equals(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID)) &&
                    info.getStatus() == HaInfo.HaStatus.SUCCESS) {
                KvBean lastestStatus = ClusterHelper.getKV(ClusterPathUtil.getHaStatusPath(info.getDhName()));
                PhysicalDNPoolSingleWH dataHost = (PhysicalDNPoolSingleWH) DbleServer.getInstance().getConfig().getDataHosts().get(info.getDhName());
                dataHost.changeIntoLastestStatus(lastestStatus.getValue());
                ClusterHelper.setKV(ClusterPathUtil.getSelfResponsePath(configValue.getKey()), ClusterPathUtil.SUCCESS);
            }
        }
    }

    @Override
    public void notifyCluster() throws Exception {
        HaConfigManager.getInstance().init();
        if (ClusterHelper.useCluster()) {
            Map<String, String> map = HaConfigManager.getInstance().getSourceJsonList();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                ClusterHelper.setKV(ClusterPathUtil.getHaStatusPath(entry.getKey()), entry.getValue());
            }
        }
    }
}
