/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen;

import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.ClusterHelper;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.logic.ClusterOperation;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterEntry;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.OnlineType;
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
    }

    @Override
    public void onInit() throws Exception {
        Map<String, OnlineType> newMap = new ConcurrentHashMap<>();
        ClusterHelper clusterHelper = ClusterHelper.getInstance(ClusterOperation.ONLINE);
        List<ClusterEntry<OnlineType>> onlineNodes = clusterHelper.getKVPath(ClusterChildMetaUtil.getOnlinePath());
        for (ClusterEntry<OnlineType> onlineNode : onlineNodes) {
            newMap.put(onlineNode.getKey(), onlineNode.getValue().getData());
        }
        for (Map.Entry<String, OnlineType> en : onlineMap.entrySet()) {
            if (!newMap.containsKey(en.getKey()) ||
                    (newMap.containsKey(en.getKey()) && !newMap.get(en.getKey()).equals(en.getValue()))) {
                deleteNode(en.getKey());
            }
        }
        onlineMap = newMap;
    }

    @Override
    public void onEvent(ClusterEvent<OnlineType> event) throws Exception {
        switch (event.getChangeType()) {
            case ADDED:
                String path = event.getPath();
                OnlineType value = event.getValue().getData();

                onlineMap.put(path, value);


                break;
            case REMOVED:
                deleteNode(event.getPath());
                break;
            default:
                break;
        }
    }


    private void deleteNode(String path) {
        onlineMap.remove(path);
        String crashNode = path.substring(path.lastIndexOf("/") + 1);
        ClusterLogic.forDDL().checkDDLAndRelease(crashNode);
        ClusterLogic.forBinlog().checkBinlogStatusRelease(crashNode);

    }


}
