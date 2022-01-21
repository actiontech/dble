/*
 * Copyright (C) 2016-2022 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.logic;

import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.path.ClusterMetaUtil;
import com.actiontech.dble.cluster.values.ClusterValue;
import com.actiontech.dble.cluster.values.FeedBackType;
import com.actiontech.dble.cluster.values.ViewChangeType;
import com.actiontech.dble.cluster.values.ViewType;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.singleton.ProxyMeta;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public class ViewClusterLogic extends AbstractClusterLogic {
    private static final Logger LOGGER = LogManager.getLogger(ViewClusterLogic.class);

    ViewClusterLogic() {
        super(ClusterOperation.VIEW);
    }

    public void executeViewEvent(String path, String key, ViewChangeType changeType) throws Exception {
        String[] childNameInfo = key.split(Repository.SCHEMA_VIEW_SPLIT);
        String schema = childNameInfo[0];
        String viewName = childNameInfo[1];

        String serverId = changeType.getInstanceName();
        String instanceName = SystemConfig.getInstance().getInstanceName();
        if (instanceName.equals(serverId)) {
            return;
        }
        String optionType = changeType.getType();
        ClusterDelayProvider.delayWhenReponseViewNotic();
        if (Repository.DELETE.equals(optionType)) {
            LOGGER.info("delete view " + path + ":" + changeType);
            if (!ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().containsKey(viewName)) {
                return;
            }

            ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().remove(viewName);
            ClusterDelayProvider.delayBeforeReponseView();
            clusterHelper.createSelfTempNode(path, FeedBackType.SUCCESS);
        } else if (Repository.UPDATE.equals(optionType)) {
            LOGGER.info("update view " + path + ":" + changeType);
            ClusterDelayProvider.delayBeforeReponseGetView();
            String stmt = clusterHelper.getPathValue(ClusterMetaUtil.getViewPath(schema, viewName)).map(ClusterValue::getData).map(ViewType::getCreateSql).orElse(null);
            if (ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().get(viewName) != null &&
                    ProxyMeta.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().get(viewName).getCreateSql().equals(stmt)) {
                ClusterDelayProvider.delayBeforeReponseView();
                clusterHelper.createSelfTempNode(path, FeedBackType.SUCCESS);
                return;
            }
            ViewMeta vm = new ViewMeta(schema, stmt, ProxyMeta.getInstance().getTmManager());
            vm.init();
            vm.addMeta(false);

            Map<String, Map<String, String>> viewCreateSqlMap = ProxyMeta.getInstance().getTmManager().getRepository().getViewCreateSqlMap();
            Map<String, String> schemaMap = viewCreateSqlMap.get(schema);
            schemaMap.put(viewName, stmt);

            ClusterDelayProvider.delayBeforeReponseView();
            clusterHelper.createSelfTempNode(path, FeedBackType.SUCCESS);
        }
    }
}
