/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.ErrorPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil.SEPARATOR;

/**
 * Created by szf on 2018/2/5.
 */
public class UViewChildResponse implements UcoreXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(UViewChildResponse.class);

    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        if (configValue.getKey().split("/").length != UcorePathUtil.getViewChangePath().split("/").length + 1) {
            //only with the type u.../d.../clu.../view/update(delete)/schema.table
            return;
        }

        String schema = configValue.getKey().split("/")[5].split(Repository.SCHEMA_VIEW_SPLIT)[0];
        String viewName = configValue.getKey().split(Repository.SCHEMA_VIEW_SPLIT)[1];
        if ("".equals(configValue.getValue())) {
            //the value of key is empty,just doing nothing
            return;
        }

        String serverId = configValue.getValue().split(Repository.SCHEMA_VIEW_SPLIT)[0];
        String optionType = configValue.getValue().split(Repository.SCHEMA_VIEW_SPLIT)[1];
        String myId = UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
        if (myId.equals(serverId) || UKvBean.DELETE.equals(configValue.getChangeType())) {
            // self node do noting
            return;
        } else {
            ClusterDelayProvider.delayWhenReponseViewNotic();
            try {
                if (Repository.DELETE.equals(optionType)) {
                    LOGGER.info("delete view " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
                    if (!DbleServer.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().containsKey(viewName)) {
                        return;
                    }
                    DbleServer.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().remove(viewName);

                    ClusterDelayProvider.delayBeforeReponseView();
                    ClusterUcoreSender.sendDataToUcore(configValue.getKey() + SEPARATOR + myId, UcorePathUtil.SUCCESS);
                } else if (Repository.UPDATE.equals(optionType)) {
                    LOGGER.info("update view " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
                    ClusterDelayProvider.delayBeforeReponseGetView();
                    String stmt = ClusterUcoreSender.getKey(UcorePathUtil.getViewPath() + SEPARATOR + schema + Repository.SCHEMA_VIEW_SPLIT + viewName).getValue();
                    if (DbleServer.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().get(viewName) != null &&
                            stmt.equals(DbleServer.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().get(viewName).getCreateSql())) {
                        ClusterDelayProvider.delayBeforeReponseView();
                        ClusterUcoreSender.sendDataToUcore(configValue.getKey() + SEPARATOR + myId, UcorePathUtil.SUCCESS);
                        return;
                    }
                    ViewMeta vm = new ViewMeta(stmt, schema, DbleServer.getInstance().getTmManager());
                    ErrorPacket error = vm.initAndSet(true, false);

                    Map<String, Map<String, String>> viewCreateSqlMap = DbleServer.getInstance().getTmManager().getRepository().getViewCreateSqlMap();
                    Map<String, String> schemaMap = viewCreateSqlMap.get(schema);
                    schemaMap.put(viewName, stmt);

                    LOGGER.info("update view result == " + error);
                    if (error != null) {
                        ClusterDelayProvider.delayBeforeReponseView();
                        ClusterUcoreSender.sendDataToUcore(configValue.getKey() + SEPARATOR + myId, new String(error.getMessage()));
                        return;
                    }

                    ClusterDelayProvider.delayBeforeReponseView();
                    ClusterUcoreSender.sendDataToUcore(configValue.getKey() + SEPARATOR + myId, UcorePathUtil.SUCCESS);
                }
            } catch (Exception e) {
                ClusterUcoreSender.sendDataToUcore(configValue.getKey() + "/" + myId, e.toString());
            }
        }
    }


    @Override
    public void notifyCluster() throws Exception {
        return;
    }
}
