/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

public final class BooleanUtil {
    private BooleanUtil() {
    }

    public static boolean parseBoolean(String val) {
        if ("true".equalsIgnoreCase(val)) {
            return true;
        } else if ("false".equalsIgnoreCase(val)) {
            return false;
        } else {
            throw new NumberFormatException("value " + val + " is not boolean value");
        }
    }
}
