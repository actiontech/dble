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
    public static final int GENERAL_LOG_FILE = 12;
    public static final int STATISTIC_TABLE_SIZE = 13;
    public static final int LOAD_DATA_NUM = 14;
    public static final int SAMPLING_RATE = 15;
    public static final int XAID_CHECK_PERIOD = 16;
    public static final int SLOW_QUERY_QUEUE_POLICY = 17;

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
                case 'L':
                case 'l':
                    return reload2LCheck(stmt, offset);
                case 'G':
                case 'g':
                    return reload2GCheck(stmt, offset);
                case 'X':
                case 'x':
                    return reload2XCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int reload2LCheck(String stmt, int offset) {
        if (stmt.length() > offset + "OAD_DATA".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'A' || c2 == 'a') && (c3 == 'D' || c3 == 'd') && (c4 == '_') &&
                    (c5 == 'D' || c5 == 'd') && (c6 == 'A' || c6 == 'a') && (c7 == 'T' || c7 == 't') &&
                    (c8 == 'A' || c8 == 'a')) {
                switch (c9) {
                    case '.':
                        return reload2LoadDataCheck(stmt, offset);
                    default:
                        return OTHER;
                }
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
                case 'A':
                case 'a':
                    return reload2SACheck(stmt, offset);
                case 'Q':
                case 'q':
                    return reload2SQCheck(stmt, offset);
                case 'L':
                case 'l':
                    return reload2SLCheck(stmt, offset);
                case 'T':
                case 't':
                    return reload2STCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // RELOAD @@samplingRate=?
    private static int reload2SACheck(String stmt, int offset) {
        if (stmt.length() > offset + 11) {
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
            if ((c1 == 'M' || c1 == 'm') && (c2 == 'P' || c2 == 'p') &&
                    (c3 == 'L' || c3 == 'l') && (c4 == 'I' || c4 == 'i') && (c5 == 'N' || c5 == 'n') &&
                    (c6 == 'G' || c6 == 'g') && c7 == 'R' && (c8 == 'A' || c8 == 'a') &&
                    (c9 == 'T' || c9 == 't') && (c10 == 'E' || c10 == 'e') && (stmt.length() > ++offset)) {
                return (offset << 8) | SAMPLING_RATE;
            }
        }
        return OTHER;
    }

    // RELOAD @@G
    private static int reload2GCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'E':
                case 'e':
                    return reload2GeCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // RELOAD @@X
    private static int reload2XCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'A':
                case 'a':
                    return reload2XaCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }

        return OTHER;
    }

    // reload @@xaIdCheck.period
    private static int reload2XaCheck(String stmt, int offset) {
        if (stmt.length() > offset + "aIdCheck.period".length()) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            char c10 = stmt.charAt(++offset);
            char c11 = stmt.charAt(++offset);
            char c12 = stmt.charAt(++offset);
            char c13 = stmt.charAt(++offset);
            char c14 = stmt.charAt(++offset);
            char c15 = stmt.charAt(++offset);
            if ((c2 == 'I' || c2 == 'i') &&
                    (c3 == 'D' || c3 == 'd') && (c4 == 'C' || c4 == 'c') && (c5 == 'H' || c5 == 'h') &&
                    (c6 == 'E' || c6 == 'e') && (c7 == 'C' || c7 == 'c') && (c8 == 'K' || c8 == 'k') &&
                    (c9 == '.') && (c10 == 'P' || c10 == 'p') && (c11 == 'E' || c11 == 'e') &&
                    (c12 == 'R' || c12 == 'r') && (c13 == 'I' || c13 == 'i') && (c14 == 'O' || c14 == 'o') &&
                    (c15 == 'D' || c15 == 'd') && (stmt.length() > ++offset)) {
                return (offset << 8) | XAID_CHECK_PERIOD;
            }
        }
        return OTHER;
    }

    // reload @@general_log.file
    private static int reload2GeCheck(String stmt, int offset) {
        if (stmt.length() > offset + "NERAL_LOG_FILE".length()) {
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
            char c11 = stmt.charAt(++offset);
            char c12 = stmt.charAt(++offset);
            char c13 = stmt.charAt(++offset);
            char c14 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'E' || c2 == 'e') &&
                    (c3 == 'R' || c3 == 'r') && (c4 == 'A' || c4 == 'a') && (c5 == 'L' || c5 == 'l') &&
                    (c6 == '_') && (c7 == 'L' || c7 == 'l') && (c8 == 'O' || c8 == 'o') &&
                    (c9 == 'G' || c9 == 'g') && (c10 == '_') && (c11 == 'F' || c11 == 'f') &&
                    (c12 == 'I' || c12 == 'i') && (c13 == 'L' || c13 == 'l') && (c14 == 'E' || c14 == 'e')) {
                return (offset << 8) | GENERAL_LOG_FILE;
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
                    (stmt.length() > offset + 1)) {
                switch (c10) {
                    case 'T':
                    case 't':
                        return slowQueryTimeCheck(stmt, offset);
                    case 'F':
                    case 'f':
                        return slowQueryFlushCheck(stmt, offset);
                    case 'Q':
                    case 'q':
                        return slowQueryQueuePolicyCheck(stmt, offset);
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

    // Statistic
    private static int reload2STCheck(String stmt, int offset) {
        if (stmt.length() > offset + 18) {
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
            char c11 = stmt.charAt(++offset);
            char c12 = stmt.charAt(++offset);
            char c13 = stmt.charAt(++offset);
            char c14 = stmt.charAt(++offset);
            char c15 = stmt.charAt(++offset);
            char c16 = stmt.charAt(++offset);
            char c17 = stmt.charAt(++offset);
            char c18 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'T' || c2 == 't') &&
                    (c3 == 'I' || c3 == 'i') && (c4 == 'S' || c4 == 's') &&
                    (c5 == 'T' || c5 == 't') && (c6 == 'I' || c6 == 'i') &&
                    (c7 == 'C' || c7 == 'c') && (c8 == '_' && c14 == '_') &&
                    (c9 == 'T' || c9 == 't') && (c10 == 'A' || c10 == 'a') &&
                    (c11 == 'B' || c11 == 'b') && (c12 == 'L' || c12 == 'l') &&
                    (c13 == 'E' || c13 == 'e') && (c15 == 'S' || c15 == 's') &&
                    (c16 == 'I' || c16 == 'i') && (c17 == 'Z' || c17 == 'z') &&
                    (c18 == 'E' || c18 == 'e')) {
                return (offset << 8) | STATISTIC_TABLE_SIZE;
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

    private static int slowQueryQueuePolicyCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
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
            char c11 = stmt.charAt(++offset);

            // reload @@slow_query.queue_policy
            if ((c1 == 'U' || c1 == 'u') && (c2 == 'E' || c2 == 'e') && (c3 == 'U' || c3 == 'u') &&
                    (c4 == 'E' || c4 == 'e') && (c5 == '_') && (c6 == 'P' || c6 == 'p') && (c7 == 'O' || c7 == 'o') &&
                    (c8 == 'L' || c8 == 'l') && (c9 == 'I' || c9 == 'i') && (c10 == 'C' || c10 == 'c') &&
                    (c11 == 'Y' || c11 == 'y') && (stmt.length() > ++offset)) {
                return (offset << 8) | SLOW_QUERY_QUEUE_POLICY;
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

    private static int reload2LoadDataCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            // show @@load_data_batch.num
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'U' || c2 == 'u') && (c3 == 'M' || c3 == 'm') &&
                    (stmt.length() > ++offset)) {
                return (offset << 8) | LOAD_DATA_NUM;
            }
        }
        return OTHER;
    }

}
