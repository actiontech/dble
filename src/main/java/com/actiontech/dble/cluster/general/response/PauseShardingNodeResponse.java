/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import com.actiontech.dble.cluster.general.listener.ClusterClearKeyListener;
import com.actiontech.dble.singleton.PauseShardingNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.actiontech.dble.cluster.general.bean.KvBean.DELETE;


public class PauseShardingNodeResponse implements ClusterXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PauseShardingNodeResponse.class);


    private Thread waitThread;

    private final Lock lock = new ReentrantLock();

    public PauseShardingNodeResponse(ClusterClearKeyListener confListener) {
        confListener.addChild(this, ClusterPathUtil.getPauseResultNodePath());
        confListener.addChild(this, ClusterPathUtil.getPauseResumePath());
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("get key in PauseShardingNodeResponse:" + configValue.getKey() + "   " + configValue.getValue());
        if (DELETE.equals(configValue.getChangeType())) {
            return;
        }
        String key = configValue.getKey();
        String value = configValue.getValue();
        if (ClusterPathUtil.getPauseResultNodePath().equals(key)) {
            waitThread = ClusterLogic.pauseShardingNodeEvent(value, lock);
        } else if (ClusterPathUtil.getPauseResumePath().equals(key)) {
            ClusterLogic.resumeShardingNodeEvent(value, lock, waitThread);
        }
    }



    /**
     * notify the cluster that the pause is over
     */
    @Override
    public void notifyCluster() throws Exception {
        lock.lock();
        try {
            if (waitThread.isAlive()) {
                waitThread.interrupt();
            }
        } finally {
            lock.unlock();
        }
        PauseShardingNodeManager.getInstance().resume();
    }
}
