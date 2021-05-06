/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.ClusterEvent;
import com.actiontech.dble.cluster.RawJson;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by szf on 2018/1/29.
 */
public class SequencePropertiesLoader extends AbstractGeneralListener<RawJson> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SequencePropertiesLoader.class);


    public SequencePropertiesLoader() {
        super(ClusterChildMetaUtil.getSequencesCommonPath());
    }

    @Override
    public void onEvent(ClusterEvent<RawJson> event) throws Exception {
        ClusterLogic.forConfig().syncSequenceJson(event.getPath(), event.getValue().getData());
    }


    @Override
    public void notifyCluster() throws Exception {
        ClusterLogic.forConfig().syncSequencePropsToCluster();
    }

}
