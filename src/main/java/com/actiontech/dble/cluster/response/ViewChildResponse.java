/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.view.Repository;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.ErrorPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.actiontech.dble.cluster.ClusterPathUtil.SEPARATOR;

/**
 * Created by szf on 2018/2/5.
 */
public class ViewChildResponse implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewChildResponse.class);

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        if (configValue.getKey().split("/").length != ClusterPathUtil.getViewChangePath().split("/").length + 1) {
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
        String myId = ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID);
        if (myId.equals(serverId) || KvBean.DELETE.equals(configValue.getChangeType())) {
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
                    ClusterHelper.setKV(configValue.getKey() + SEPARATOR + myId, ClusterPathUtil.SUCCESS);
                } else if (Repository.UPDATE.equals(optionType)) {
                    LOGGER.info("update view " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
                    ClusterDelayProvider.delayBeforeReponseGetView();
                    String stmt = ClusterHelper.getKV(ClusterPathUtil.getViewPath() + SEPARATOR + schema + Repository.SCHEMA_VIEW_SPLIT + viewName).getValue();
                    if (DbleServer.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().get(viewName) != null &&
                            stmt.equals(DbleServer.getInstance().getTmManager().getCatalogs().get(schema).getViewMetas().get(viewName).getCreateSql())) {
                        ClusterDelayProvider.delayBeforeReponseView();
                        ClusterHelper.setKV(configValue.getKey() + SEPARATOR + myId, ClusterPathUtil.SUCCESS);
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
                        ClusterHelper.setKV(configValue.getKey() + SEPARATOR + myId, new String(error.getMessage()));
                        return;
                    }

                    ClusterDelayProvider.delayBeforeReponseView();
                    ClusterHelper.setKV(configValue.getKey() + SEPARATOR + myId, ClusterPathUtil.SUCCESS);
                }
            } catch (Exception e) {
                ClusterHelper.setKV(configValue.getKey() + "/" + myId, e.toString());
            }
        }
    }


    @Override
    public void notifyCluster() throws Exception {
        return;
    }
}
