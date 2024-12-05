/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.route.parser;

import com.oceanbase.obsharding_d.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ManagerParseStart {
    private ManagerParseStart() {
    }

    public static final int OTHER = -1;
    public static final int STATISTIC_QUEUE_MONITOR = 1;

    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
                case ' ':
                    continue;
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    continue;
                case '@':
                    return startCheck(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // STATISTIC_QUEUE_TIMER
    private static int startCheck(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@') {
            if (stmt.length() > offset + 21 && stmt.substring(offset + 1).toUpperCase().startsWith("STATISTIC_QUEUE_MONITOR")) {
                return offset + 24 << 8 | STATISTIC_QUEUE_MONITOR;
            }
        }
        return OTHER;
    }

}
