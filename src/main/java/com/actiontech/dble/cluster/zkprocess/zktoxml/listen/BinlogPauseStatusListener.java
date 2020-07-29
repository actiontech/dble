/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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
 * Created by huqing.yan on 2017/5/25.
 */
public class BinlogPauseStatusListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusListener.class);


    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("event happen:" + event.toString());
        }
        switch (event.getType()) {
            case CHILD_ADDED: {
                ChildData childData = event.getData();
                LOGGER.info("childEvent " + childData.getPath() + " " + event.getType());
                String value = new String(childData.getData(), StandardCharsets.UTF_8);
                ClusterLogic.executeBinlogPauseEvent(value);
            }
            break;
            case CHILD_UPDATED:
                break;
            case CHILD_REMOVED: {
                ChildData childData = event.getData();
                LOGGER.info("childEvent " + childData.getPath() + " " + event.getType());
                String value = new String(childData.getData(), StandardCharsets.UTF_8);
                ClusterLogic.executeBinlogPauseDeleteEvent(value);
            }
            break;
            default:
                break;
        }
    }

}
