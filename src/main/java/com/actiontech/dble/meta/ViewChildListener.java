/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.alibaba.fastjson.JSONObject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static com.actiontech.dble.backend.mysql.view.Repository.*;


public class ViewChildListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewChildListener.class);

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData childData = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED:
                createOrUpdateViewMeta(childData, false);
                break;
            case CHILD_UPDATED:
                createOrUpdateViewMeta(childData, true);
                break;
            case CHILD_REMOVED:
                deleteNode(childData);
                break;
            default:
                break;
        }
    }


    /**
     * delete the view data from view meta
     *
     * @param childData
     */
    private void deleteNode(ChildData childData) throws Exception {
        String path = childData.getPath();
        String[] paths = path.split("/");
        String schema = paths[paths.length - 1].split(":")[0];
        String viewName = paths[paths.length - 1].split(":")[1];

        DbleServer.getInstance().getTmManager().addMetaLock(schema, viewName, "DROP VIEW " + viewName);
        try {
            DbleServer.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().remove(viewName);
        } finally {
            DbleServer.getInstance().getTmManager().removeMetaLock(schema, viewName);
        }

    }


    /**
     * update the meta if the view updated
     */
    private void createOrUpdateViewMeta(ChildData childData, boolean isReplace) throws Exception {
        String path = childData.getPath();
        String[] paths = path.split("/");
        String jsonValue = new String(childData.getData(), StandardCharsets.UTF_8);
        JSONObject obj = (JSONObject) JSONObject.parse(jsonValue);

        //if the view is create or replace by this server it self
        String serverId = obj.getString(SERVER_ID);
        if (serverId.equals(ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID))) {
            return;
        }
        String createSql = obj.getString(CREATE_SQL);
        String schema = paths[paths.length - 1].split(SCHEMA_VIEW_SPLIT)[0];

        ViewMeta vm = new ViewMeta(createSql, schema, DbleServer.getInstance().getTmManager());
        vm.initAndSet(isReplace, false);

    }


}
