/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen;

import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.RawJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2019/10/30.
 */
public class DbGroupStatusListener extends AbstractGeneralListener<RawJson> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbGroupStatusListener.class);

    public DbGroupStatusListener() {
        super(ClusterChildMetaUtil.getHaStatusPath());
    }

    @Override
    public void onEvent(ClusterEvent<RawJson> event) throws Exception {
        switch (event.getChangeType()) {
            case ADDED: {
                if (event.isUpdate()) {
                    updateStatus(event);
                }
            }
            break;
            case REMOVED:
                break;
            default:
                break;
        }
    }


    private void updateStatus(ClusterEvent<RawJson> childData) {
        try {
            String dbGroupName = childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
            RawJson rawJson = childData.getValue().getData();
            ClusterLogic.forHA().dbGroupChangeEvent(dbGroupName, rawJson);
        } catch (Exception e) {
            LOGGER.warn("get Error when update Ha status", e);
        }
    }


}
