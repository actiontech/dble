/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.singleton.ClusterGeneralConfig;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.ConfStatus;
import com.actiontech.dble.manager.response.ReloadConfig;
import com.actiontech.dble.manager.response.RollbackConfig;
import com.actiontech.dble.meta.ReloadManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.actiontech.dble.meta.ReloadStatus.TRIGGER_TYPE_CLUSTER;

/**
 * Created by szf on 2018/1/31.
 */
public class ConfigStatusResponse implements ClusterXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusResponse.class);

    private static final String CONFIG_PATH = ClusterPathUtil.getConfStatusPath();

    public ConfigStatusResponse(ClusterClearKeyListener confListener) {
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(KvBean pathValue) throws Exception {

        ClusterDelayProvider.delayAfterGetNotice();
        if (DbleServer.getInstance().getFrontProcessors() != null) {
            //step 1 check if the change is from itself
            LOGGER.info("notify " + pathValue.getKey() + " " + pathValue.getValue() + " " + pathValue.getChangeType());
            ConfStatus status = new ConfStatus(pathValue.getValue());
            if (status.getFrom().equals(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
                //self node
                return;
            }

            //check if the reload is already be done by this node
            if (!"".equals(ClusterHelper.getKV(ClusterPathUtil.getSelfConfStatusPath()).getValue()) ||
                    "".equals(ClusterHelper.getKV(ClusterPathUtil.getConfStatusPath()).getValue())) {
                return;
            }

            //step 2 check the change type /rollback /reload
            if (status.getStatus() == ConfStatus.Status.ROLLBACK) {
                LOGGER.info("rollback " + pathValue.getKey() + " " + pathValue.getValue() + " " + pathValue.getChangeType());
                try {
                    ClusterDelayProvider.delayBeforeSlaveRollback();
                    LOGGER.info("rollback " + pathValue.getKey() + " " + pathValue.getValue() + " " + pathValue.getChangeType());
                    try {
                        boolean result = RollbackConfig.rollback(TRIGGER_TYPE_CLUSTER);
                        if (!checkLocalResult(result)) {
                            return;
                        }
                    } catch (Exception e) {
                        throw e;
                    } finally {
                        ReloadManager.reloadFinish();
                    }

                    ClusterDelayProvider.delayAfterSlaveRollback();
                    LOGGER.info("rollback config: sent config status success to ucore start");
                    ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(), ClusterPathUtil.SUCCESS);
                    LOGGER.info("rollback config: sent config status success to ucore end");
                } catch (Exception e) {
                    String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                    LOGGER.info("rollback config: sent config status failed to ucore start");
                    ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(), errorinfo);
                    LOGGER.info("rollback config: sent config status failed to ucore end");
                }
                return;
            }

            //step 3 reload the config and set the self config status
            try {
                ClusterDelayProvider.delayBeforeSlaveReload();
                if (status.getStatus() == ConfStatus.Status.RELOAD_ALL) {
                    LOGGER.info("reload_all " + pathValue.getKey() + " " + pathValue.getValue() + " " + pathValue.getChangeType());
                    final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
                    lock.writeLock().lock();
                    try {
                        if (!ReloadManager.startReload(TRIGGER_TYPE_CLUSTER, ConfStatus.Status.RELOAD_ALL)) {
                            LOGGER.info("reload config failed because self is in reloading");
                            ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(),
                                    "Reload status error ,other client or cluster may in reload");
                            return;
                        }
                        try {
                            boolean result = ReloadConfig.reloadAll(Integer.parseInt(status.getParams()));
                            if (!checkLocalResult(result)) {
                                return;
                            }
                        } catch (Exception e) {
                            throw e;
                        } finally {
                            ReloadManager.reloadFinish();
                        }

                    } finally {
                        lock.writeLock().unlock();
                    }
                } else {
                    LOGGER.info("reload " + pathValue.getKey() + " " + pathValue.getValue() + " " + pathValue.getChangeType());
                    final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
                    lock.writeLock().lock();
                    try {
                        if (!ReloadManager.startReload(TRIGGER_TYPE_CLUSTER, ConfStatus.Status.RELOAD)) {
                            LOGGER.info("reload config failed because self is in reloading");
                            ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(),
                                    "Reload status error ,other client or cluster may in reload");
                            return;
                        }
                        try {
                            boolean result = ReloadConfig.reload();
                            if (!checkLocalResult(result)) {
                                return;
                            }
                        } catch (Exception e) {
                            throw e;
                        } finally {
                            ReloadManager.reloadFinish();
                        }

                    } finally {
                        lock.writeLock().unlock();
                    }
                }
                ClusterDelayProvider.delayAfterSlaveReload();
                LOGGER.info("reload config: sent config status success to ucore start");
                ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(), ClusterPathUtil.SUCCESS);
                LOGGER.info("reload config: sent config status success to ucore end");
            } catch (Exception e) {
                String errorinfo = e.getMessage() == null ? e.toString() : e.getMessage();
                LOGGER.info("reload config: sent config status failed to ucore start");
                ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(), errorinfo);
                LOGGER.info("reload config: sent config status failed to ucore end");
            }
        }
    }


    public boolean checkLocalResult(boolean result) throws Exception {
        if (!result) {
            LOGGER.info("reload config: sent config status success to ucore start");
            ClusterDelayProvider.delayAfterSlaveReload();
            ClusterHelper.setKV(ClusterPathUtil.getSelfConfStatusPath(), "interrupt by command.should reload config again");
        }
        return result;
    }

    @Override
    public void notifyCluster() throws Exception {

    }
}
