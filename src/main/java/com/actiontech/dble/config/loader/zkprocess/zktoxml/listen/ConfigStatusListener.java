/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.manager.response.ReloadConfig;
import com.actiontech.dble.manager.response.RollbackConfig;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by huqing.yan on 2017/6/23.
 */
public class ConfigStatusListener extends ZkMultiLoader implements NotifyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigStatusListener.class);
    public static final String SUCCESS = "SUCCESS";
    private final String currZkPath;
    private Set<NotifyService> childService = new HashSet<>();

    public ConfigStatusListener(ZookeeperProcessListen zookeeperListen, CuratorFramework curator) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfStatusPath();
        zookeeperListen.addWatch(currZkPath, this);
    }

    public void addChild(NotifyService service) {
        childService.add(service);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        ClusterDelayProvider.delayAfterGetNotice();
        if (DbleServer.getInstance().getFrontProcessors() != null) {
            String value = this.getDataToString(currZkPath);
            ConfStatus status = new ConfStatus(value);
            if (status.getFrom().equals(ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
                return true; //self node
            }
            LOGGER.info("ConfigStatusListener notifyProcess zk to object  :" + status);
            if (status.getStatus() == ConfStatus.Status.ROLLBACK) {
                try {
                    ClusterDelayProvider.delayBeforeSlaveRollback();
                    RollbackConfig.rollback();
                    ClusterDelayProvider.delayAfterSlaveRollback();
                    LOGGER.info("rollback config: sent config status success to zk start");
                    ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                            SUCCESS.getBytes(StandardCharsets.UTF_8));
                    LOGGER.info("rollback config: sent config status success to zk end");
                } catch (Exception e) {
                    String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                    LOGGER.info("rollback config: sent config status failed to zk start");
                    ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID),
                            errorinfo.getBytes(StandardCharsets.UTF_8));
                    LOGGER.info("rollback config: sent config status failed to zk end");
                }

                return true;
            }
            for (NotifyService service : childService) {
                try {
                    service.notifyProcess();
                } catch (Exception e) {
                    LOGGER.warn("ConfigStatusListener notify  error :" + service + " ,Exception info:", e);
                }
            }
            try {
                ClusterDelayProvider.delayBeforeSlaveReload();
                LOGGER.info("reload config: ready to reload config");
                if (status.getStatus() == ConfStatus.Status.RELOAD_ALL) {
                    ReloadConfig.reloadAll(Integer.parseInt(status.getParams()));
                } else {
                    ReloadConfig.reload();
                }
                ClusterDelayProvider.delayAfterSlaveReload();
                LOGGER.info("reload config: sent config status success to zk start");
                ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), SUCCESS.getBytes(StandardCharsets.UTF_8));
                LOGGER.info("reload config: sent config status success to zk end");
            } catch (Exception e) {
                String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                LOGGER.info("reload config: sent config status failed to zk start");
                ZKUtils.createTempNode(KVPathUtil.getConfStatusPath(), ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID), errorinfo.getBytes(StandardCharsets.UTF_8));
                LOGGER.info("reload config: sent config status failed to zk end");
            }
        }
        return true;
    }
}
