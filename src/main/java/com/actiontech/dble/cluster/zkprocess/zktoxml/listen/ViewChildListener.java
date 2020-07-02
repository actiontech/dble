/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterLogic;
import com.actiontech.dble.cluster.ClusterPathUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


public class ViewChildListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewChildListener.class);
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("event happen:" + event.toString());
        }
        switch (event.getType()) {
            case CHILD_ADDED:
                ChildData childData = event.getData();
                ClusterDelayProvider.delayWhenReponseViewNotic();
                LOGGER.info("childEvent " + childData.getPath() + " " + event.getType());
                executeViewOperator(childData);
                break;
            case CHILD_UPDATED:
                break;
            case CHILD_REMOVED:
                break;
            default:
                break;
        }
    }

    private void executeViewOperator(ChildData childData) throws Exception {
        String path = childData.getPath();
        String value = new String(childData.getData(), StandardCharsets.UTF_8);
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        String key = paths[paths.length - 1];
        ClusterLogic.executeViewEvent(path, key, value);
    }


}
