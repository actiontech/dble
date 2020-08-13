/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.model.SystemConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by huqing.yan on 2017/6/23.
 */
public class ConfigStatusListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigStatusListener.class);
    private Set<NotifyService> childService = new HashSet<>();

    public ConfigStatusListener(Set<NotifyService> childService) {
        this.childService = childService;
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("event happen:" + event.toString());
        }
        switch (event.getType()) {
            case CHILD_ADDED:
                ChildData childData = event.getData();
                LOGGER.info("childEvent " + childData.getPath() + " " + event.getType());
                executeStatusChange(childData);
                break;
            case CHILD_UPDATED:
                break;
            case CHILD_REMOVED:
                break;
            default:
                break;
        }
    }

    private void executeStatusChange(ChildData childData) throws Exception {
        if (!DbleServer.getInstance().isStartup()) {
            return;
        }
        ClusterDelayProvider.delayAfterGetNotice();
        String value = new String(childData.getData(), StandardCharsets.UTF_8);

        ConfStatus status = new ConfStatus(value);
        if (status.getFrom().equals(SystemConfig.getInstance().getInstanceName())) {
            return; //self node
        }
        LOGGER.info("ConfigStatusListener notifyProcess zk to object  :" + status);
        for (NotifyService service : childService) {
            try {
                service.notifyProcess();
            } catch (Exception e) {
                LOGGER.warn("ConfigStatusListener notify  error :" + service + " ,Exception info:", e);
            }
        }
        ClusterLogic.reloadConfigEvent(value, status.getParams());
    }
}
