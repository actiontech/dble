/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen;

import com.oceanbase.obsharding_d.btrace.provider.ClusterDelayProvider;
import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.ViewChangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ViewChildListener extends AbstractGeneralListener<ViewChangeType> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ViewChildListener.class);

    public ViewChildListener() {
        super(ClusterChildMetaUtil.getViewChangePath());
    }

    @Override
    public void onEvent(ClusterEvent<ViewChangeType> event) throws Exception {
        switch (event.getChangeType()) {
            case ADDED:
                ClusterDelayProvider.delayWhenReponseViewNotic();
                LOGGER.info("childEvent " + event.getPath() + " " + event.getChangeType());
                executeViewOperator(event);
                break;
            case REMOVED:
                break;
            default:
                break;
        }

    }


    private void executeViewOperator(ClusterEvent<ViewChangeType> childData) throws Exception {
        String path = childData.getPath();
        ViewChangeType data = childData.getValue().getData();
        String[] paths = path.split(ClusterPathUtil.SEPARATOR);
        String key = paths[paths.length - 1];
        ClusterLogic.forView().executeViewEvent(path, key, data);
    }


}
