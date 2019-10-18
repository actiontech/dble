package com.actiontech.dble.config.loader.zkprocess.zktoxml.listen;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDNPoolSingleWH;
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
public class DataHostStatusListener implements PathChildrenCacheListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataHostStatusListener.class);

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData childData = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED:
                break;
            case CHILD_UPDATED:
                updateStatus(childData);
                break;
            case CHILD_REMOVED:
                break;
            default:
                break;
        }
    }

    private void updateStatus(ChildData childData) {
        String nodeName = childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
        String data = new String(childData.getData(), StandardCharsets.UTF_8);
        PhysicalDNPoolSingleWH dataHost = (PhysicalDNPoolSingleWH) DbleServer.getInstance().getConfig().getDataHosts().get(nodeName);
        dataHost.changeIntoLastestStatus(data);
    }

}
