/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.cluster.general.response;

import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.GeneralListener;
import com.oceanbase.obsharding_d.cluster.general.listener.ClusterClearKeyListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.ClusterValue;
import com.oceanbase.obsharding_d.cluster.values.PauseInfo;
import com.oceanbase.obsharding_d.singleton.PauseShardingNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.oceanbase.obsharding_d.cluster.values.ChangeType.REMOVED;


public class PauseShardingNodeResponse extends AbstractGeneralListener<PauseInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PauseShardingNodeResponse.class);


    private Thread waitThread;

    private final Lock lock = new ReentrantLock();


    public PauseShardingNodeResponse() {
        super(ClusterChildMetaUtil.getPauseShardingNodePath());
    }


    @Override
    public final GeneralListener<PauseInfo> registerPrefixForUcore(ClusterClearKeyListener confListener) {
        confListener.addChild(this, ClusterPathUtil.getPauseResultNodePath());
        confListener.addChild(this, ClusterPathUtil.getPauseResumePath());
        return this;
    }

    @Override
    public void onEvent(ClusterEvent<PauseInfo> event) throws Exception {
        if (REMOVED.equals(event.getChangeType())) {
            return;
        }
        String key = event.getPath();
        ClusterValue<PauseInfo> value = event.getValue();
        if (ClusterPathUtil.getPauseResultNodePath().equals(key)) {
            waitThread = ClusterLogic.forPauseResume().pauseShardingNodeEvent(value.getData(), lock);
        } else if (ClusterPathUtil.getPauseResumePath().equals(key)) {
            ClusterLogic.forPauseResume().resumeShardingNodeEvent(value.getData(), lock, waitThread);
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
