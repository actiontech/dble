/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.listener;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.ClusterValue;
import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.values.OnlineType;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.OnlineStatus;
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
    private volatile Map<String, OnlineType> onlineMap = new ConcurrentHashMap<>();
    private long index = 0;
    private AbstractConsulSender sender;

    public ClusterOffLineListener(AbstractConsulSender sender) {
        this.sender = sender;
    }

    public Map<String, OnlineType> copyOnlineMap() {
        return new ConcurrentHashMap<>(onlineMap);
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
                SubscribeReturnBean output = sender.subscribeKvPrefix(request);
                if (output.getIndex() == index) {
                    if (lackSelf) {
                        lackSelf = !reInitOnlineStatus();
                    }
                    continue;
                }
                //LOGGER.debug("the index of the single key "+path+" is "+index);
                Map<String, OnlineType> newMap = new ConcurrentHashMap<>();
                for (int i = 0; i < output.getKeysCount(); i++) {
                    newMap.put(output.getKeys(i), ClusterValue.readFromJson(output.getValues(i), OnlineType.class).getData());
                }

                for (Map.Entry<String, OnlineType> en : onlineMap.entrySet()) {
                    if (!newMap.containsKey(en.getKey()) ||
                            (newMap.containsKey(en.getKey()) && !newMap.get(en.getKey()).equals(en.getValue()))) {
                        String crashNode = en.getKey().split("/")[en.getKey().split("/").length - 1];
                        ClusterLogic.forDDL().checkDDLAndRelease(crashNode);
                        ClusterLogic.forBinlog().checkBinlogStatusRelease(crashNode);
                    }
                }
                String instanceName = SystemConfig.getInstance().getInstanceName();
                String selfPath = ClusterPathUtil.getOnlinePath(instanceName);
                if (!newMap.containsKey(selfPath)) {
                    lackSelf = !reInitOnlineStatus();
                    newMap.put(selfPath, OnlineStatus.getInstance().toOnlineType());
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
            boolean success = OnlineStatus.getInstance().rebuildOnline();
            if (success) {
                LOGGER.info("rewrite server online status success or cluster not ye inited");
            } else {
                LOGGER.info("rewrite server online failed ");
            }
            return success;
        } catch (Exception e) {
            LOGGER.warn("rewrite server online status failed: ", e);
            //alert
            return false;
        }
    }
}
