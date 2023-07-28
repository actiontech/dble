/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.listener;

import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.cluster.general.bean.SubscribeRequest;
import com.actiontech.dble.cluster.general.bean.SubscribeReturnBean;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.path.ClusterPathUtil;
import com.actiontech.dble.cluster.values.ClusterValue;
import com.actiontech.dble.cluster.values.OnlineType;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.OnlineStatus;
import com.actiontech.dble.util.exception.DetachedException;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.cluster.path.ClusterPathUtil.SEPARATOR;

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
                if (sender.isDetach()) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
                    index = 0;
                    continue;
                }
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
                if (sender.isDetach()) {
                    continue;
                }
                //LOGGER.debug("the index of the single key "+path+" is "+index);
                Map<String, OnlineType> newMap = new ConcurrentHashMap<>();
                for (int i = 0; i < output.getKeysCount(); i++) {
                    if (Strings.isEmpty(output.getValues(i))) {
                        continue;
                    }
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
            } catch (DetachedException e) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            } catch (IOException e) {
                if (!sender.isDetach()) {
                    LOGGER.info("error in deal with key,may be the ucore is shut down", e);
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
            } catch (Throwable e) {
                LOGGER.info("error in deal with key,may be the ucore is shut down", e);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2000));
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
