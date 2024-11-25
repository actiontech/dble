/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.mysql;

public final class CursorTypeFlags {
    private CursorTypeFlags() {
    }


    public static final int CURSOR_TYPE_NO_CURSOR = 0;
    public static final int CURSOR_TYPE_READ_ONLY = 1;
    public static final int CURSOR_TYPE_FOR_UPDATE = 2;
    public static final int CURSOR_TYPE_SCROLLABLE = 4;
}
