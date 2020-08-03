/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.listener;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.bean.SubscribeRequest;
import com.actiontech.dble.cluster.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.kVtoXml.ClusterToXml;
import com.actiontech.dble.cluster.response.ClusterXmlLoader;
import com.actiontech.dble.cluster.response.DdlChildResponse;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.BinlogPause;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.PauseInfo;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.OnlineStatus;
import com.actiontech.dble.singleton.PauseDatanodeManager;
import com.actiontech.dble.singleton.ProxyMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.actiontech.dble.cluster.ClusterPathUtil.SEPARATOR;

/**
 * Created by szf on 2018/2/8.
 */
public class ClusterOffLineListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterOffLineListener.class);
    private volatile Map<String, String> onlineMap = new ConcurrentHashMap<>();
    private long index = 0;


    public Map<String, String> copyOnlineMap() {
        return new ConcurrentHashMap<>(onlineMap);
    }

    private void checkDDLAndRelease(String serverId) {
        //deal with the status when the ddl is init notified
        //and than the ddl server is shutdown
        for (Map.Entry<String, String> en : DdlChildResponse.getLockMap().entrySet()) {
            if (serverId.equals(en.getValue())) {
                ProxyMeta.getInstance().getTmManager().removeMetaLock(en.getKey().split("\\.")[0], en.getKey().split("\\.")[1]);
                DdlChildResponse.getLockMap().remove(en.getKey());
                ClusterHelper.cleanPath(ClusterPathUtil.getDDLPath(en.getKey()));
            }
        }
    }

    private void checkBinlogStatusRelease(String serverId) {
        try {
            //check the latest bing_log status
            KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getBinlogPauseLockPath());
            if ("".equals(lock.getValue()) || serverId.equals(lock.getValue())) {
                DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
            }
            KvBean status = ClusterHelper.getKV(ClusterPathUtil.getBinlogPauseStatus());
            if (!"".equals(status.getValue())) {
                BinlogPause pauseInfo = new BinlogPause(status.getValue());
                if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.ON && serverId.equals(pauseInfo.getFrom())) {
                    ClusterHelper.cleanPath(ClusterPathUtil.getBinlogPauseStatus() + "/");
                    ClusterHelper.setKV(ClusterPathUtil.getBinlogPauseStatus(), (new BinlogPause("", BinlogPause.BinlogPauseStatus.OFF)).toString());
                    ClusterHelper.cleanKV(ClusterPathUtil.getBinlogPauseLockPath());
                }
            }
        } catch (Exception e) {
            LOGGER.warn(" server offline binlog status check error: ", e);
        }
    }

    private void checkPauseStatusRelease(String serverId) {
        try {
            KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getPauseShardingNodePath());
            boolean needRelease = false;
            if (!"".equals(lock.getValue())) {
                PauseInfo pauseInfo = new PauseInfo(lock.getValue());
                if (pauseInfo.getFrom().equals(serverId)) {
                    needRelease = true;
                }
            } else if (PauseDatanodeManager.getInstance().getIsPausing().get()) {
                needRelease = true;
            }
            if (needRelease) {
                ClusterXmlLoader loader = ClusterToXml.getListener().getReponse(ClusterPathUtil.getPauseShardingNodePath());
                loader.notifyCluster();
            }

        } catch (Exception e) {
            LOGGER.warn(" server offline binlog status check error: ", e);
        }
    }


    @Override
    public void run() {
        boolean lackSelf = false;
        for (; ; ) {
            try {
                SubscribeRequest request = new SubscribeRequest();
                request.setIndex(index);
                request.setDuration(60);
                request.setPath(ClusterPathUtil.getOnlinePath() + SEPARATOR);
                SubscribeReturnBean output = ClusterHelper.subscribeKvPrefix(request);
                if (output.getIndex() == index) {
                    if (lackSelf) {
                        lackSelf = !reInitOnlineStatus();
                    }
                    continue;
                }
                //LOGGER.debug("the index of the single key "+path+" is "+index);
                Map<String, String> newMap = new ConcurrentHashMap<>();
                for (int i = 0; i < output.getKeysCount(); i++) {
                    newMap.put(output.getKeys(i), output.getValues(i));
                }

                for (Map.Entry<String, String> en : onlineMap.entrySet()) {
                    if (!newMap.containsKey(en.getKey()) ||
                            (newMap.containsKey(en.getKey()) && !newMap.get(en.getKey()).equals(en.getValue()))) {
                        String serverId = en.getKey().split("/")[en.getKey().split("/").length - 1];
                        checkDDLAndRelease(serverId);
                        checkBinlogStatusRelease(serverId);
                        checkPauseStatusRelease(serverId);
                    }
                }
                String instanceId = SystemConfig.getInstance().getInstanceId();
                String selfPath = ClusterPathUtil.getOnlinePath(instanceId);
                if (!newMap.containsKey(selfPath)) {
                    lackSelf = !reInitOnlineStatus();
                    newMap.put(selfPath, instanceId);
                }
                onlineMap = newMap;
                index = output.getIndex();
            } catch (Exception e) {
                LOGGER.warn("error in offline listener: ", e);
            }
        }
    }

    private boolean reInitOnlineStatus() {
        try {
            //release and renew lock
            boolean init = OnlineStatus.getInstance().rebuildOnline();
            if (init) {
                LOGGER.info("rewrite server online status success");
            } else {
                LOGGER.info("rewrite server wait for online status inited");
            }
            return true;
        } catch (Exception e) {
            LOGGER.warn("rewrite server online status failed: ", e);
            //alert
            return false;
        }
    }
}
