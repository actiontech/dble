package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterLogic;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Created by szf on 2019/10/30.
 */
public class DbGroupStatusListener implements PathChildrenCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupStatusListener.class);

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("event happen:" + event.toString());
        }
        switch (event.getType()) {
            case CHILD_ADDED:
            case CHILD_UPDATED:
                updateStatus(event.getData());
                break;
            case CHILD_REMOVED:
                break;
            default:
                break;
        }
    }

    private void updateStatus(ChildData childData) {
        try {
            String dbGroupName = childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
            String value = new String(childData.getData(), StandardCharsets.UTF_8);
            ClusterLogic.dbGroupChangeEvent(dbGroupName, value);
        } catch (Exception e) {
            LOGGER.warn("get Error when update Ha status", e);
        }
    }


}
