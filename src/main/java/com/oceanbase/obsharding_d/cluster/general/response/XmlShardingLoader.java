/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.response;

import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.RawJson;

/**
 * Created by szf on 2018/1/26.
 */
public class XmlShardingLoader extends AbstractGeneralListener<RawJson> {

    public XmlShardingLoader() {
        super(ClusterChildMetaUtil.getConfShardingPath());
    }


    @Override
    public void onEvent(ClusterEvent<RawJson> event) throws Exception {
        ClusterLogic.forConfig().syncShardingJson(event.getPath(), event.getValue().getData());
    }


    @Override
    public void notifyCluster() throws Exception {
        ClusterLogic.forConfig().syncShardingXmlToCluster();
    }


}
