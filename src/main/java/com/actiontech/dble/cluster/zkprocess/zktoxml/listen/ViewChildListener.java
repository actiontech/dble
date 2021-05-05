/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.zkprocess.zktoxml.listen;

import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.cluster.AbstractGeneralListener;
import com.actiontech.dble.cluster.ClusterChildMetaUtil;
import com.actiontech.dble.cluster.ClusterEvent;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.logic.ClusterLogic;
import com.actiontech.dble.cluster.values.ViewChangeType;
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
