/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen;

import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.HaInfo;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by szf on 2019/10/30.
 */
public class DbGroupResponseListener extends AbstractGeneralListener<HaInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupResponseListener.class);

    public DbGroupResponseListener() {
        super(ClusterChildMetaUtil.getHaResponseChildPath());
    }

    @Override
    public void onEvent(ClusterEvent<HaInfo> event) throws Exception {
        switch (event.getChangeType()) {
            case ADDED:
                updateStatus(event);
                break;
            case REMOVED:
                break;
            default:
                break;
        }
    }


    private void updateStatus(ClusterEvent<HaInfo> childData) throws Exception {
        HaInfo haInfo = childData.getValue().getData();
        LOGGER.info("Ha disable node " + childData.getPath() + " updated , and data is " + childData.getValue());
        String[] paths = childData.getPath().split(ClusterPathUtil.SEPARATOR);
        String dbGroupName = paths[paths.length - 1];

        try {
            ClusterLogic.forHA().dbGroupResponseEvent(haInfo, dbGroupName);
        } catch (Exception e) {
            LOGGER.warn("get error when try to response to the disable");
            ZKUtils.createTempNode(childData.getPath(), SystemConfig.getInstance().getInstanceName(), e.getMessage().getBytes());
        }
    }

}
