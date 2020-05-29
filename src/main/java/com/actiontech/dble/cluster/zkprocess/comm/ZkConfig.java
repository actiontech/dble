/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.comm;

import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.zkprocess.zktoxml.ZktoXmlMain;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.OnlineStatus;
import com.actiontech.dble.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


/**
 * ZkConfig
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public final class ZkConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkConfig.class);

    private ZkConfig() {
    }

    private static ZkConfig zkCfgInstance = new ZkConfig();


    public static void initZk() {
        try {
            ZktoXmlMain.loadZktoFile();
            tryDeleteOldOnline();
            // online
            ZKUtils.createOnline(ClusterPathUtil.getOnlinePath(), SystemConfig.getInstance().getInstanceName(), OnlineStatus.getInstance());
        } catch (Exception e) {
            LOGGER.error("error:", e);
        }
    }

    /**
     * @return
     * @Created 2016/9/15
     */
    public static ZkConfig getInstance() {
        return zkCfgInstance;
    }



    private static void tryDeleteOldOnline() throws Exception {
        //try to delete online
        if (ZKUtils.getConnection().checkExists().forPath(ClusterPathUtil.getOnlinePath(SystemConfig.getInstance().getInstanceName())) != null) {
            byte[] info;
            try {
                info = ZKUtils.getConnection().getData().forPath(ClusterPathUtil.getOnlinePath(SystemConfig.getInstance().getInstanceName()));
            } catch (Exception e) {
                LOGGER.info("can not get old online from zk,just do as it not exists");
                return;
            }
            String oldOnlne = new String(info, StandardCharsets.UTF_8);
            if (OnlineStatus.getInstance().canRemovePath(oldOnlne)) {
                LOGGER.warn("remove online from zk path ,because has same IP & serverPort");
                ZKUtils.getConnection().delete().forPath(ClusterPathUtil.getOnlinePath(SystemConfig.getInstance().getInstanceName()));
            } else {
                throw new RuntimeException("Online path with other IP or serverPort exist,make sure different instance has different myid");
            }
        }
    }

}
