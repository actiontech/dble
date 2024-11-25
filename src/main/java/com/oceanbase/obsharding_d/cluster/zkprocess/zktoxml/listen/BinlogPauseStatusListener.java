/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.listen;

import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.logic.ClusterLogic;
import com.oceanbase.obsharding_d.cluster.path.ClusterChildMetaUtil;
import com.oceanbase.obsharding_d.cluster.values.ClusterEvent;
import com.oceanbase.obsharding_d.cluster.values.Empty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by huqing.yan on 2017/5/25.
 */
public class BinlogPauseStatusListener extends AbstractGeneralListener<Empty> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogPauseStatusListener.class);

    public BinlogPauseStatusListener() {
        super(ClusterChildMetaUtil.getBinlogPausePath());
    }

    @Override
    public void onEvent(ClusterEvent<Empty> event) throws Exception {
        switch (event.getChangeType()) {
            case ADDED:
                LOGGER.info("childEvent " + event.getPath() + " " + event.getChangeType());
                ClusterLogic.forBinlog().executeBinlogPauseEvent(event);
                break;
            case REMOVED:
                LOGGER.info("childEvent " + event.getPath() + " " + event.getChangeType());
                ClusterLogic.forBinlog().executeBinlogPauseDeleteEvent(event);
                break;
            default:
                break;
        }
    }


}
