/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.general.response;

import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.path.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.values.ClusterEvent;
import com.actiontech.dble.cluster.values.RawJson;


/**
 * Created by szf on 2018/1/29.
 */
public class SequencePropertiesLoader extends AbstractGeneralListener<RawJson> {

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
