/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.information.tables;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.meta.ColumnMeta;
import com.actiontech.dble.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;

public class DbleClusterRenewThread extends ManagerBaseTable {


    public DbleClusterRenewThread() {
        super("dble_cluster_renew_thread", 1);
    }

    @Override
    protected void initColumnAndType() {
        columns.put("renew_thread", new ColumnMeta("renew_thread", "varchar(200)", false));
        columnsType.put("renew_thread", Fields.FIELD_TYPE_VAR_STRING);
    }

    @Override
    protected List<LinkedHashMap<String, String>> getRows() {
        List<LinkedHashMap<String, String>> result = Lists.newArrayList();
        if (ClusterConfig.getInstance().isClusterEnable() &&
                ClusterGeneralConfig.getInstance().getClusterSender() instanceof AbstractConsulSender) {

            AbstractConsulSender clusterSender = (AbstractConsulSender) ClusterGeneralConfig.getInstance().getClusterSender();
            clusterSender.fetchRenewThread().stream().
                    forEach(c -> {
                        LinkedHashMap<String, String> m = Maps.newLinkedHashMap();
                        m.put("renew_thread", c);
                        result.add(m);
                    });
        }
        return result;
    }
}
