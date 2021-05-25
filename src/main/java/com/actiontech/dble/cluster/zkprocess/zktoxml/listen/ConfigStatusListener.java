/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.path.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.values.ClusterEvent;
import com.actiontech.dble.cluster.values.ConfStatus;
import com.actiontech.dble.cluster.zkprocess.comm.NotifyService;
import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by huqing.yan on 2017/6/23.
 */
public class ConfigStatusListener extends AbstractGeneralListener<ConfStatus> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigStatusListener.class);
    private Set<NotifyService> childService = new HashSet<>();

    public ConfigStatusListener(Set<NotifyService> childService) {
        super(ClusterChildMetaUtil.getConfStatusPath());
        this.childService = childService;
    }


    @Override
    public void onEvent(ClusterEvent<ConfStatus> event) throws Exception {
        switch (event.getChangeType()) {
            case ADDED:
                LOGGER.info("childEvent " + event.getPath() + " " + event.getChangeType());
                executeStatusChange(event);
                break;
            case REMOVED:
                break;
            default:
                break;
        }
    }


    private void executeStatusChange(ClusterEvent<ConfStatus> childData) throws Exception {
        if (!DbleServer.getInstance().isStartup()) {
            return;
        }
        ClusterDelayProvider.delayAfterGetNotice();

        ConfStatus status = childData.getValue().getData();
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
        ClusterLogic.forConfig().reloadConfigEvent(status, status.getParams());
    }
}
