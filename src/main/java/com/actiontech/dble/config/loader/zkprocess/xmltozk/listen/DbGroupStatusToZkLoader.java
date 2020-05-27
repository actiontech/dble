package com.actiontech.dble.config.loader.zkprocess.xmltozk.listen;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.singleton.HaConfigManager;
import org.apache.curator.framework.CuratorFramework;

import java.util.Map;

/**
 * Created by szf on 2019/10/30.
 */
public class DbGroupStatusToZkLoader extends ZkMultiLoader implements NotifyService {


    public DbGroupStatusToZkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        zookeeperListen.addToInit(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        HaConfigManager.getInstance().init();
        if (ClusterConfig.getInstance().isNeedSyncHa()) {
            Map<String, String> map = HaConfigManager.getInstance().getSourceJsonList();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                this.checkAndWriteString(ClusterPathUtil.getHaStatusPath() + ClusterPathUtil.SEPARATOR, entry.getKey(), entry.getValue());
            }
        }
        return true;
    }
}
