/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ManagerParseReload {
    private ManagerParseReload() {
    }

    public static final int OTHER = -1;
    public static final int CONFIG = 1;
    public static final int USER_STAT = 4;
    //public static final int CONFIG_ALL = 5;
    public static final int SQL_SLOW = 6;
    public static final int META_DATA = 7;
    public static final int QUERY_CF = 8;
    public static final int SLOW_QUERY_TIME = 9;
    public static final int SLOW_QUERY_FLUSH_PERIOD = 10;
    public static final int SLOW_QUERY_FLUSH_SIZE = 11;

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
                    return reload2Check(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int reload2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@' && stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'C':
                case 'c':
                    return reload2CCheck(stmt, offset);
                case 'U':
                case 'u':
                    return reload2UCheck(stmt, offset);
                case 'S':
                case 's':
                    return reload2SCheck(stmt, offset);
                case 'Q':
                case 'q':
                    return reload2QCheck(stmt, offset);
                case 'M':
                case 'm':
                    return reload2MCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static boolean isConfig(String stmt, int offset) {
        if (stmt.length() == ++offset)
            return true;

        char c1 = stmt.charAt(offset);
        return (c1 == ' ') || (c1 == '\t') || (c1 == '\r') || (c1 == '\n') || (c1 == '_') || (c1 == '-');

    }

    // RELOAD @@CONFIG
    // Please comply with the specification: offset retain the last position that has been checked.
    private static int reload2CCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'N' || c2 == 'n') && (c3 == 'F' || c3 == 'f') &&
                    (c4 == 'I' || c4 == 'i') && (c5 == 'G' || c5 == 'g')) {
                if (isConfig(stmt, offset)) {
                    return (offset << 8) | CONFIG;
                }
            }
        }
        return OTHER;
    }

    // RELOAD @@USER
    private static int reload2UCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'S' || c1 == 's') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r')) {
                if (stmt.length() > offset + 5) {
                    char c6 = stmt.charAt(++offset);
                    char c7 = stmt.charAt(++offset);
                    char c8 = stmt.charAt(++offset);
                    char c9 = stmt.charAt(++offset);
                    char c10 = stmt.charAt(++offset);
                    if ((c6 == '_' || c6 == '-') && (c7 == 'S' || c7 == 's') && (c8 == 'T' || c8 == 't') &&
                            (c9 == 'A' || c9 == 'a') && (c10 == 'T' || c10 == 't')) {
                        return USER_STAT;
                    }
                }
                return OTHER;
            }
        }
        return OTHER;
    }


    // RELOAD @@S
    private static int reload2SCheck(String stmt, int offset) {
        if (stmt.length() > offset + 1) {

            switch (stmt.charAt(++offset)) {
                case 'Q':
                case 'q':
                    return reload2SQCheck(stmt, offset);
                case 'L':
                case 'l':
                    return reload2SLCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // RELOAD @@SQL
    private static int reload2SLCheck(String stmt, int offset) {
        if (stmt.length() > offset + 10) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            char c10 = stmt.charAt(++offset);

            // reload @@slow_query
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'W' || c2 == 'w') && (c3 == '_') &&
                    (c4 == 'Q' || c4 == 'q') && (c5 == 'U' || c5 == 'u') && (c6 == 'E' || c6 == 'e') &&
                    (c7 == 'R' || c7 == 'r') && (c8 == 'Y' || c8 == 'y') && (c9 == '.') &&
                    (stmt.length() > offset)) {
                switch (c10) {
                    case 'T':
                    case 't':
                        return slowQueryTimeCheck(stmt, offset);
                    case 'F':
                    case 'f':
                        return slowQueryFlushCheck(stmt, offset);
                    default:
                        return OTHER;
                }
            }
        }
        return OTHER;
    }

    // RELOAD @@SQL
    private static int reload2SQCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);

            // reload @@sqlslow
            if ((c1 == 'L' || c1 == 'l') && (c2 == 's' || c2 == 'S') &&
                    (c3 == 'L' || c3 == 'l') && (c4 == 'O' || c4 == 'o') && (c5 == 'W' || c5 == 'w') &&
                    (stmt.length() > ++offset)) {
                return (offset << 8) | SQL_SLOW;
            }
        }
        return OTHER;
    }

    private static int slowQueryTimeCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);

            // reload @@slow_query.time
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'M' || c2 == 'm') && (c3 == 'E' || c3 == 'e') &&
                    (stmt.length() > ++offset)) {
                return (offset << 8) | SLOW_QUERY_TIME;
            }
        }
        return OTHER;
    }

    private static int slowQueryFlushCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);

            // reload @@slowquery.flush
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'U' || c2 == 'u') && (c3 == 's' || c3 == 'S') &&
                    (c4 == 'H' || c4 == 'h')) {
                switch (stmt.charAt(++offset)) {
                    case 'S':
                    case 's':
                        return slowQueryFlushSizeCheck(stmt, offset);
                    case 'p':
                    case 'P':
                        return slowQueryFlushPeriodCheck(stmt, offset);
                    default:
                        return OTHER;
                }
            }

            return OTHER;
        }
        return OTHER;
    }

    private static int slowQueryFlushSizeCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);

            // reload @@slow_query.flushsize
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'Z' || c2 == 'z') &&
                    (c3 == 'E' || c3 == 'e') && (stmt.length() > ++offset)) {
                return (offset << 8) | SLOW_QUERY_FLUSH_SIZE;
            }
        }
        return OTHER;
    }

    private static int slowQueryFlushPeriodCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);

            // reload @@slow_query.flushperiod
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'R' || c2 == 'r') && (c3 == 'I' || c3 == 'i') &&
                    (c4 == 'O' || c4 == 'o') && (c5 == 'D' || c5 == 'd') && (stmt.length() > ++offset)) {
                return (offset << 8) | SLOW_QUERY_FLUSH_PERIOD;
            }
        }
        return OTHER;
    }

    // RELOAD @@METADATA WHERE sharding=? and table=?
    private static int reload2MCheck(String stmt, int offset) {
        if (stmt.length() > offset + 7) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);

            if ((c1 == 'E' || c1 == 'e') && (c2 == 'T' || c2 == 't') && (c3 == 'A' || c3 == 'a') &&
                    (c4 == 'D' || c4 == 'd') && (c5 == 'A' || c5 == 'a') && (c6 == 'T' || c6 == 't') && (c7 == 'A' || c7 == 'a')) {
                // skip space
                for (++offset; offset < stmt.length(); ++offset) {
                    if (!ParseUtil.isSpace(stmt.charAt(offset))) {
                        break;
                    }
                }

                if (stmt.length() == offset) {
                    return (offset << 8) | META_DATA;
                } else if (offset + 5 < stmt.length()) {
                    char c8 = stmt.charAt(offset);
                    char c9 = stmt.charAt(++offset);
                    char c10 = stmt.charAt(++offset);
                    char c11 = stmt.charAt(++offset);
                    char c12 = stmt.charAt(++offset);
                    if ((c8 == 'W' || c8 == 'w') && (c9 == 'H' || c9 == 'h') && (c10 == 'E' || c10 == 'e') &&
                            (c11 == 'R' || c11 == 'r') && (c12 == 'E' || c12 == 'e')) {
                        return (++offset << 8) | META_DATA;
                    }
                }

                return OTHER;
            }
        }
        return OTHER;
    }

    // RELOAD @@QUERY
    private static int reload2QCheck(String stmt, int offset) {
        if (stmt.length() > offset + 7) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);

            // include "RELOAD @@QUERY_CF"
            if ((c1 == 'U' || c1 == 'u') && (c2 == 'E' || c2 == 'e') && (c3 == 'R' || c3 == 'r') &&
                    (c4 == 'Y' || c4 == 'y') && (c5 == '_') && (c6 == 'C' || c6 == 'c') && (c7 == 'F' || c7 == 'f') &&
                    stmt.trim().length() == ++offset) {
                return QUERY_CF;
            }

            // exclude "RELOAD @@QUERY_CF =  ";
            int index = stmt.indexOf("=");
            if (index != -1 && !ParseUtil.isErrorTail(0, stmt.substring(offset, index)) && stmt.trim().length() > ++index) {
                return QUERY_CF;
            }
        }
        return OTHER;
    }
}
