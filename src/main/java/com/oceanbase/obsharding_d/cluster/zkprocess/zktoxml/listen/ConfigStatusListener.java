/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.btrace.provider.ClusterDelayProvider;
import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.ConfStatus;
import com.oceanbase.obsharding_d.cluster.zkprocess.comm.NotifyService;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
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
        if (!OBsharding_DServer.getInstance().isStartup()) {
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
