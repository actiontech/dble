/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

public final class BooleanUtil {
    private static final String TRUE = "true";
    private static final String FALSE = "false";
    private BooleanUtil() {
    }

    public static boolean parseBoolean(String val) {
        if (TRUE.equalsIgnoreCase(val)) {
            return true;
        } else if (FALSE.equalsIgnoreCase(val)) {
            return false;
        } else {
            throw new NumberFormatException("value " + val + " is not boolean value");
        }
    }

    public static boolean isBoolean(String val) {
        return TRUE.equalsIgnoreCase(val) || FALSE.equalsIgnoreCase(val);
    }
}
