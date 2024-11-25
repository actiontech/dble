/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.logic;

import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author dcy
 * Create Date: 2021-04-30
 */
public class GeneralClusterLogic {
    private static final Logger LOGGER = LogManager.getLogger(GeneralClusterLogic.class);

    GeneralClusterLogic() {

    }


    public int getPathHeight(String path) {
        //todo:optimize here
        if (path.endsWith(ClusterPathUtil.SEPARATOR)) {
            path = path.substring(0, path.length() - 1);
        }
        return path.split(ClusterPathUtil.SEPARATOR).length;
    }
}
