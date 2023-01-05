/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.service;

/**
 * response to the results of front connection
 */
public enum ResultFlag {
    OK,
    EOF_ROW,
    ERROR,
    OTHER // Default
}
