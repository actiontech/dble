/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.route.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ManagerParseOnOff {
    private ManagerParseOnOff() {
    }

    public static final int OTHER = -1;
    public static final int SLOW_QUERY_LOG = 1;

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
                    return atCheck(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // enable/disable @@LOG_SLOW_QUERY
    private static int atCheck(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@' &&
                stmt.length() > offset + "SLOW_QUERY_LOG".length()) {
            String prefix = stmt.substring(++offset).toUpperCase();
            if (prefix.startsWith("SLOW_QUERY_LOG") && (stmt.length() == offset + 14 || ParseUtil.isEOF(stmt, offset + 14))) {
                return SLOW_QUERY_LOG;
            }
        }
        return OTHER;
    }
}
