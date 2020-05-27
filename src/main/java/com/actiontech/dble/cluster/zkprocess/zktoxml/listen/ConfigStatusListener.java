/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.cluster.zkprocess.comm.ZookeeperProcessListen;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.cluster.zkprocess.zookeeper.process.ZkMultiLoader;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.manager.response.ReloadConfig;
import com.actiontech.dble.manager.response.RollbackConfig;
import com.actiontech.dble.meta.ReloadManager;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static com.actiontech.dble.meta.ReloadStatus.TRIGGER_TYPE_CLUSTER;

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
        currZkPath = ClusterPathUtil.getConfStatusPath();
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
            if (status.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
                return true; //self node
            }
            LOGGER.info("ConfigStatusListener notifyProcess zk to object  :" + status);
            if (status.getStatus() == ConfStatus.Status.ROLLBACK) {
                try {
                    ClusterDelayProvider.delayBeforeSlaveRollback();
                    try {
                        boolean result = RollbackConfig.rollback(TRIGGER_TYPE_CLUSTER);
                        if (!checkLocalResult(result)) {
                            return true;
                        }
                    } catch (Exception e) {
                        throw e;
                    } finally {
                        ReloadManager.reloadFinish();
                    }

                    ClusterDelayProvider.delayAfterSlaveRollback();
                    LOGGER.info("rollback config: sent config status success to zk start");
                    ZKUtils.createTempNode(ClusterPathUtil.getConfStatusPath(), SystemConfig.getInstance().getInstanceName(),
                            SUCCESS.getBytes(StandardCharsets.UTF_8));
                    LOGGER.info("rollback config: sent config status success to zk end");
                } catch (Exception e) {
                    String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                    LOGGER.info("rollback config: sent config status failed to zk start");
                    ZKUtils.createTempNode(ClusterPathUtil.getConfStatusPath(), SystemConfig.getInstance().getInstanceName(),
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
                if (!ReloadManager.startReload(TRIGGER_TYPE_CLUSTER, ConfStatus.Status.RELOAD_ALL)) {
                    LOGGER.info("reload config failed because self is in reloading");
                    ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(),
                            "Reload status error ,other client or cluster may in reload");
                    return true;
                }
                boolean result;
                try {
                    result = ReloadConfig.reloadAll(Integer.parseInt(status.getParams()));
                    if (!checkLocalResult(result)) {
                        return true;
                    }
                } catch (Exception e) {
                    throw e;
                } finally {
                    ReloadManager.reloadFinish();
                }
                ClusterDelayProvider.delayAfterSlaveReload();
                LOGGER.info("reload config: sent config status success to zk start");
                ZKUtils.createTempNode(ClusterPathUtil.getConfStatusPath(), SystemConfig.getInstance().getInstanceName(), SUCCESS.getBytes(StandardCharsets.UTF_8));
                LOGGER.info("reload config: sent config status success to zk end");
            } catch (Exception e) {
                String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                LOGGER.info("reload config: sent config status failed to zk start");
                ZKUtils.createTempNode(ClusterPathUtil.getConfStatusPath(), SystemConfig.getInstance().getInstanceName(), errorinfo.getBytes(StandardCharsets.UTF_8));
                LOGGER.info("reload config: sent config status failed to zk end");
            }
        }
        return true;
    }


    private boolean checkLocalResult(boolean result) throws Exception {
        if (!result) {
            LOGGER.info("reload config: sent config status success to ucore start");
            ZKUtils.createTempNode(ClusterPathUtil.getConfStatusPath(), SystemConfig.getInstance().getInstanceName(),
                    "interrupt by command.should reload config again".getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }
}
