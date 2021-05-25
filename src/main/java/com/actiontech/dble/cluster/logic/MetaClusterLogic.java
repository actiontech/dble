/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.logic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public class MetaClusterLogic extends AbstractClusterLogic {
    private static final Logger LOGGER = LogManager.getLogger(MetaClusterLogic.class);

    MetaClusterLogic() {
        super(ClusterOperation.META);
    }


}
