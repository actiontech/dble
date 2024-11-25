/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

/**
 * @author dcy
 * Create Date: 2021-04-16
 */
public enum ServiceTaskType {
    NORMAL,
    NOTIFICATION,
    CLOSE,
    DELAYED,
    SSL;
}
