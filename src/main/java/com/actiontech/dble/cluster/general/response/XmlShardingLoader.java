/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.path.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.values.ClusterEvent;
import com.actiontech.dble.cluster.values.RawJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/1/26.
 */
public class XmlShardingLoader extends AbstractGeneralListener<RawJson> {
    private static final Logger LOGGER = LoggerFactory.getLogger(XmlShardingLoader.class);

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
