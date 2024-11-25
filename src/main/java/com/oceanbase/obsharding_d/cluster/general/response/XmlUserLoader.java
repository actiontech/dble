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

public class XmlUserLoader extends AbstractGeneralListener<RawJson> {

    public XmlUserLoader() {
        super(ClusterChildMetaUtil.getUserConfPath());
    }


    @Override
    public void onEvent(ClusterEvent<RawJson> event) throws Exception {
        ClusterLogic.forConfig().syncUserJson(event.getPath(), event.getValue().getData());
    }


    @Override
    public void notifyCluster() throws Exception {
        ClusterLogic.forConfig().syncUserXmlToCluster();
    }
}
