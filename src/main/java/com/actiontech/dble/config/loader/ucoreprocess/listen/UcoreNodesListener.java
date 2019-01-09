/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess.listen;


import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.UcoreInterface;
import com.actiontech.dble.backend.mysql.view.CKVStoreRepository;
import com.actiontech.dble.backend.mysql.view.FileSystemRepository;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.server.status.OnlineLockStatus;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by szf on 2018/4/27.
 */
public class UcoreNodesListener implements Runnable {


    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreNodesListener.class);
    private long index = 0;

    @Override
    public void run() {
        for (; ; ) {
            try {
                UcoreInterface.SubscribeNodesInput subscribeNodesInput = UcoreInterface.SubscribeNodesInput.newBuilder().
                        setDuration(60).setIndex(index).build();
                UcoreInterface.SubscribeNodesOutput output = ClusterUcoreSender.subscribeNodes(subscribeNodesInput);
                if (index != output.getIndex()) {
                    index = output.getIndex();
                    List<String> ips = new ArrayList<>();
                    for (int i = 0; i < output.getIpsList().size(); i++) {
                        ips.add(output.getIps(i));
                    }
                    UcoreConfig.getInstance().setIpList(ips);
                    UcoreConfig.getInstance().setIp(StringUtils.join(ips, ','));
                }

                if (DbleServer.getInstance().getTmManager().getRepository() instanceof FileSystemRepository) {
                    LOGGER.warn("Dble first reconnect to ucore ,local view repository change to CKVStoreRepository");
                    Repository newViewRepository = new CKVStoreRepository();
                    DbleServer.getInstance().getTmManager().setRepository(newViewRepository);
                    Map<String, Map<String, String>> viewCreateSqlMap = newViewRepository.getViewCreateSqlMap();
                    DbleServer.getInstance().getTmManager().reloadViewMeta(viewCreateSqlMap);
                    //init online status
                    LOGGER.warn("Dble first reconnect to ucore ,online status rebuild");
                    OnlineLockStatus.getInstance().metaUcoreInit(true);
                }
            } catch (Exception e) {
                LOGGER.warn("error in ucore nodes watch,try for another time");
            }
        }
    }
}
