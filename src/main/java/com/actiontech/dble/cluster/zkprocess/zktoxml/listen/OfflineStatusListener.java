/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.logic.ClusterOperation;
import com.actiontech.dble.cluster.path.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.values.ClusterEntry;
import com.actiontech.dble.cluster.values.ClusterEvent;
import com.actiontech.dble.cluster.values.OnlineType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OfflineStatusListener extends AbstractGeneralListener<OnlineType> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OfflineStatusListener.class);
    private volatile Map<String, OnlineType> onlineMap = new ConcurrentHashMap<>();

    public Map<String, OnlineType> copyOnlineMap() {
        return new ConcurrentHashMap<>(onlineMap);
    }

    public OfflineStatusListener() throws Exception {
        super(ClusterChildMetaUtil.getOnlinePath());
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.ONLINE);
        List<ClusterEntry<OnlineType>> onlineNodes = clusterHelper.getKVPath(ClusterChildMetaUtil.getOnlinePath());
        for (ClusterEntry<OnlineType> onlineNode : onlineNodes) {
            onlineMap.put(onlineNode.getKey(), onlineNode.getValue().getData());
        }
    }


    @Override
    public void onEvent(ClusterEvent<OnlineType> event) throws Exception {
        switch (event.getChangeType()) {
            case ADDED:
                //noinspection deprecation
            case UPDATED:
                String path = event.getPath();
                OnlineType value = event.getValue().getData();

                onlineMap.put(path, value);


                break;
            case REMOVED:
                deleteNode(event);
                break;
            default:
                break;
        }
    }


    private void deleteNode(ClusterEvent<OnlineType> event) {
        String path = event.getPath();
        onlineMap.remove(path);
        String crashNode = path.substring(path.lastIndexOf("/") + 1);
        ClusterLogic.forDDL().checkDDLAndRelease(crashNode);
        ClusterLogic.forBinlog().checkBinlogStatusRelease(crashNode);

    }


}
