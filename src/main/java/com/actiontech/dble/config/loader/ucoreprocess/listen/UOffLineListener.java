package com.actiontech.dble.config.loader.ucoreprocess.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.UcoreInterface;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.KVtoXml.UcoreToXml;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreXmlLoader;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.loader.UDdlChildResponse;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.BinlogPause;
import com.actiontech.dble.config.loader.zkprocess.zookeeper.process.PauseInfo;
import com.actiontech.dble.server.status.OnlineLockStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil.SEPARATOR;

/**
 * Created by szf on 2018/2/8.
 */
public class UOffLineListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UOffLineListener.class);
    private volatile Map<String, String> onlineMap = new HashMap<>();
    private long index = 0;


    public Map<String, String> copyOnlineMap() {
        return new HashMap<>(onlineMap);
    }

    private void checkDDLAndRelease(String serverId) {
        //deal with the status when the ddl is init notified
        //and than the ddl server is shutdown
        for (Map.Entry<String, String> en : UDdlChildResponse.getLockMap().entrySet()) {
            if (serverId.equals(en.getValue())) {
                DbleServer.getInstance().getTmManager().removeMetaLock(en.getKey().split("\\.")[0], en.getKey().split("\\.")[1]);
                UDdlChildResponse.getLockMap().remove(en.getKey());
                ClusterUcoreSender.deleteKVTree(UcorePathUtil.getDDLPath(en.getKey()) + "/");
            }
        }
    }

    private void checkBinlogStatusRelease(String serverId) {
        try {
            //check the latest bing_log status
            UKvBean lock = ClusterUcoreSender.getKey(UcorePathUtil.getBinlogPauseLockPath());
            if ("".equals(lock.getValue()) || serverId.equals(lock.getValue())) {
                DbleServer.getInstance().getBackupLocked().compareAndSet(true, false);
            }
            UKvBean status = ClusterUcoreSender.getKey(UcorePathUtil.getBinlogPauseStatus());
            if (!"".equals(status.getValue())) {
                BinlogPause pauseInfo = new BinlogPause(status.getValue());
                if (pauseInfo.getStatus() == BinlogPause.BinlogPauseStatus.ON && serverId.equals(pauseInfo.getFrom())) {
                    ClusterUcoreSender.deleteKVTree(UcorePathUtil.getBinlogPauseStatus() + "/");
                    ClusterUcoreSender.sendDataToUcore(UcorePathUtil.getBinlogPauseStatus(), (new BinlogPause("", BinlogPause.BinlogPauseStatus.OFF)).toString());
                    ClusterUcoreSender.deleteKV(UcorePathUtil.getBinlogPauseLockPath());
                }
            }
        } catch (Exception e) {
            LOGGER.warn(" server offline binlog status check error");
        }
    }

    private void checkPauseStatusRelease(String serverId) {
        try {
            UKvBean lock = ClusterUcoreSender.getKey(UcorePathUtil.getPauseDataNodePath());
            boolean needRelease = false;
            if (!"".equals(lock.getValue())) {
                PauseInfo pauseInfo = new PauseInfo(lock.getValue());
                if (pauseInfo.getFrom().equals(serverId)) {
                    needRelease = true;
                }
            } else if (DbleServer.getInstance().getMiManager().getIsPausing().get()) {
                needRelease = true;
            }
            if (needRelease) {
                UcoreXmlLoader loader = UcoreToXml.getListener().getReponse(UcorePathUtil.getPauseDataNodePath());
                loader.notifyCluster();
            }

        } catch (Exception e) {
            LOGGER.warn(" server offline binlog status check error");
        }
    }


    @Override
    public void run() {
        boolean lackSelf = false;
        for (; ; ) {
            try {
                UcoreInterface.SubscribeKvPrefixInput input
                        = UcoreInterface.SubscribeKvPrefixInput.newBuilder().
                        setIndex(index).setDuration(60).
                        setKeyPrefix(UcorePathUtil.getOnlinePath() + SEPARATOR).build();
                UcoreInterface.SubscribeKvPrefixOutput output = ClusterUcoreSender.subscribeKvPrefix(input);
                if (output.getIndex() == index) {
                    if (lackSelf) {
                        lackSelf = !reInitOnlineStatus();
                    }
                    continue;
                }
                //LOGGER.debug("the index of the single key "+path+" is "+index);
                Map<String, String> newMap = new HashMap<>();
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
                String serverId = UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
                String selfPath = UcorePathUtil.getOnlinePath(serverId);
                if (!newMap.containsKey(selfPath)) {
                    lackSelf = !reInitOnlineStatus();
                    newMap.put(selfPath, serverId);
                }
                onlineMap = newMap;
                index = output.getIndex();
            } catch (Exception e) {
                LOGGER.warn("error in offline listener :", e);
            }
        }
    }

    private boolean reInitOnlineStatus() {
        try {
            //release and renew lock
            boolean init = OnlineLockStatus.getInstance().metaUcoreInit(false);
            if (init) {
                LOGGER.info("rewrite server online status success");
            } else {
                LOGGER.info("rewrite server wait for online status inited");
            }
            return true;
        } catch (Exception e) {
            LOGGER.warn("rewrite server online status failed", e);
            //alert
            return false;
        }
    }
}
