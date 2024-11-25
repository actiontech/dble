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
