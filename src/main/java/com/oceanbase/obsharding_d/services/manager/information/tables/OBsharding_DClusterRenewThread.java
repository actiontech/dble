/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.information.tables;

import com.oceanbase.obsharding_d.cluster.ClusterGeneralConfig;
import com.oceanbase.obsharding_d.cluster.general.AbstractConsulSender;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.meta.ColumnMeta;
import com.oceanbase.obsharding_d.services.manager.information.ManagerBaseTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;

public class OBsharding_DClusterRenewThread extends ManagerBaseTable {


    public OBsharding_DClusterRenewThread() {
        super("obsharding_d_cluster_renew_thread", 1);
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
