/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

/**
 * response to the results of front connection
 */
public enum ResultFlag {
    OK,
    EOF_ROW,
    ERROR,
    OTHER // Default
}
