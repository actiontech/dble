/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UDistributeLock;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;

import java.io.IOException;

public final class GlobalStatus {
    private UDistributeLock onlineLock = null;

    private GlobalStatus() {
    }

    private static final GlobalStatus INSTANCE = new GlobalStatus();

    public static GlobalStatus getInstance() {
        return INSTANCE;
    }

    public boolean metaUcoreInit(boolean init) throws IOException {
        //check if the online mark is on than delete the mark and renew it
        ClusterUcoreSender.deleteKV(UcorePathUtil.getOnlinePath(UcoreConfig.getInstance().
                getValue(ClusterParamCfg.CLUSTER_CFG_MYID)));
        if (onlineLock != null) {
            onlineLock.release();
        } else if (!init) {
            return false;
        }
        onlineLock = new UDistributeLock(UcorePathUtil.getOnlinePath(UcoreConfig.getInstance().
                getValue(ClusterParamCfg.CLUSTER_CFG_MYID)),
                UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
        if (!onlineLock.acquire()) {
            throw new IOException("set online status failed");
        }
        return true;
    }

}
