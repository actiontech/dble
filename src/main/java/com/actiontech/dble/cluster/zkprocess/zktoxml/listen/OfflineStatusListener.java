/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.general.bean.KvBean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OfflineStatusListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(OfflineStatusListener.class);
    private volatile Map<String, String> onlineMap = new ConcurrentHashMap<>();
    public Map<String, String> copyOnlineMap() {
        return new ConcurrentHashMap<>(onlineMap);
    }
    public OfflineStatusListener() throws Exception {
        List<KvBean> onlineNodes = ClusterHelper.getKVPath(ClusterPathUtil.getOnlinePath());
        for (KvBean onlineNode : onlineNodes) {
            onlineMap.put(onlineNode.getKey(), onlineNode.getValue());
        }
    }
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("event happen:" + event.toString());
        }
        ChildData childData = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED:
                String path = childData.getPath();
                String value = new String(childData.getData(), StandardCharsets.UTF_8);
                onlineMap.put(path, value);
                break;
            case CHILD_UPDATED:
                break;
            case CHILD_REMOVED:
                deleteNode(childData);
                break;
            default:
                break;
        }
    }

    private void deleteNode(ChildData childData) {
        String path = childData.getPath();
        onlineMap.remove(path);
        String crashNode = path.substring(path.lastIndexOf("/") + 1);
        ClusterLogic.checkDDLAndRelease(crashNode);
        ClusterLogic.checkBinlogStatusRelease(crashNode);
        ClusterLogic.checkPauseStatusRelease(crashNode);
    }


}
