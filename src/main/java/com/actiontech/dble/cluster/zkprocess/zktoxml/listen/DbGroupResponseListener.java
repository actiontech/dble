package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


/**
 * Created by szf on 2019/10/30.
 */
public class DbGroupResponseListener implements PathChildrenCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupResponseListener.class);

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("event happen:" + event.toString());
        }
        switch (event.getType()) {
            case CHILD_ADDED:
                updateStatus(event.getData());
                break;
            case CHILD_UPDATED:
                break;
            case CHILD_REMOVED:
                break;
            default:
                break;
        }
    }


    private void updateStatus(ChildData childData) throws Exception {
        String value = new String(childData.getData(), StandardCharsets.UTF_8);
        LOGGER.info("Ha disable node " + childData.getPath() + " updated , and data is " + value);
        String[] paths = childData.getPath().split(ClusterPathUtil.SEPARATOR);
        String dbGroupName = paths[paths.length - 1];
        while (StringUtil.isEmpty(value)) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
            value = ClusterHelper.getPathValue(childData.getPath());
        }

        try {
            ClusterLogic.dbGroupResponseEvent(value, dbGroupName);
        } catch (Exception e) {
            LOGGER.warn("get error when try to response to the disable");
            ZKUtils.createTempNode(childData.getPath(), SystemConfig.getInstance().getInstanceName(), e.getMessage().getBytes());
        }
    }

}
