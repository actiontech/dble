/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.config.model.SystemConfig;
import com.google.common.io.Files;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.io.File;
import java.io.IOException;

/**
 * Created by magicdoom on 2016/10/27.
 */
public class BinDataPathChildrenCacheListener implements PathChildrenCacheListener {
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData data = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED:
                add(data, false);
                break;
            case CHILD_UPDATED:
                add(data, true);
                break;
            default:
                break;
        }
    }

    private void add(ChildData childData, boolean reload) throws IOException {
        String name = childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
        byte[] data = childData.getData();
        File file = new File(
                SystemConfig.getHomePath() + File.separator + "conf",
                name);
        Files.write(data, file);
    }

}
