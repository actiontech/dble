/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

/**
 * @author dcy
 * Create Date: 2021-05-19
 */
public enum WriteFlag {
    END_OF_QUERY,
    END_OF_SESSION,
    PARK_OF_MULTI_QUERY,

    //useless
    FLUSH;


}
