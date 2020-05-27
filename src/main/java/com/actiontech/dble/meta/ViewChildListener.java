/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.singleton.ProxyMeta;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.nio.charset.StandardCharsets;

import static com.actiontech.dble.backend.mysql.view.Repository.*;


public class ViewChildListener implements PathChildrenCacheListener {

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData childData = event.getData();
        ClusterDelayProvider.delayWhenReponseViewNotic();
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

        ProxyMeta.getInstance().getTmManager().addMetaLock(schema, viewName, "DROP VIEW " + viewName);
        try {
            ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().remove(viewName);
        } finally {
            ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, viewName);
        }

    }

    /**
     * update the meta if the view updated
     */
    private void createOrUpdateViewMeta(ChildData childData, boolean isReplace) throws Exception {
        String path = childData.getPath();
        String[] paths = path.split("/");
        String jsonValue = new String(childData.getData(), StandardCharsets.UTF_8);
        JsonObject obj = new JsonParser().parse(jsonValue).getAsJsonObject();

        //if the view is create or replace by this server it self
        String serverId = obj.get(SERVER_ID).getAsString();
        if (serverId.equals(SystemConfig.getInstance().getInstanceName())) {
            return;
        }
        String createSql = obj.get(CREATE_SQL).getAsString();
        String schema = paths[paths.length - 1].split(SCHEMA_VIEW_SPLIT)[0];

        ViewMeta vm = new ViewMeta(schema, createSql, ProxyMeta.getInstance().getTmManager());
        vm.init(isReplace);
        vm.addMeta(false);

    }

}
