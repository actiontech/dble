/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.route.parser;

import com.oceanbase.obsharding_d.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ManagerParseOnOff {
    private ManagerParseOnOff() {
    }

    public static final int OTHER = -1;
    public static final int SLOW_QUERY_LOG = 1;
    public static final int ALERT = 2;
    public static final int CUSTOM_MYSQL_HA = 3;
    public static final int CAP_CLIENT_FOUND_ROWS = 4;
    public static final int GENERAL_LOG = 5;
    public static final int STATISTIC = 6;
    public static final int LOAD_DATA_BATCH = 7;
    public static final int SQLDUMP_SQL = 8;
    public static final int MEMORY_BUFFER_MONITOR = 9;


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

    private static int atCheck(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@' && stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'A':
                case 'a':
                    return aCheck(stmt, offset);
                case 'S':
                case 's':
                    return sCheck(stmt, offset);
                case 'C':
                case 'c':
                    return cCheck(stmt, offset);
                case 'G':
                case 'g':
                    return gCheck(stmt, offset);
                case 'L':
                case 'l':
                    return lCheck(stmt, offset);
                case 'm':
                case 'M':
                    return mCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // enable/disable @@ALERT
    private static int aCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            String prefix = stmt.substring(offset).toUpperCase();
            if (prefix.startsWith("ALERT") && (stmt.length() == offset + 5 || ParseUtil.isEOF(stmt, offset + 5))) {
                return ALERT;
            }
        }
        return OTHER;
    }

    // enable/disable  @@memory_buffer_monitor
    private static int mCheck(String stmt, int offset) {
        final String keyword = "MEMORY_BUFFER_MONITOR";
        if (ParseUtil.compare(stmt, offset, keyword) && ParseUtil.isEOF(stmt, offset + keyword.length())) {
            return MEMORY_BUFFER_MONITOR;
        }
        return OTHER;
    }

    // enable/disable  @@load_data_batch;
    private static int lCheck(String stmt, int offset) {
        if (stmt.length() > offset + 14) {
            String prefix = stmt.substring(offset).toUpperCase();
            if (prefix.startsWith("LOAD_DATA_BATCH") && (stmt.length() == offset + 15 || ParseUtil.isEOF(stmt, offset + 14))) {
                return LOAD_DATA_BATCH;
            }
        }
        return OTHER;
    }

    private static int sCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'L':
                case 'l':
                    return slCheck(stmt, offset);
                case 'T':
                case 't':
                    return stCheck(stmt, offset);
                case 'Q':
                case 'q':
                    return sqCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // enable/disable @@SLOW_QUERY_LOG
    private static int slCheck(String stmt, int offset) {
        if (stmt.length() > offset + 12) {
            String prefix = stmt.substring(offset).toUpperCase();
            if (prefix.startsWith("LOW_QUERY_LOG") && (stmt.length() == offset + 13 || ParseUtil.isEOF(stmt, offset + 13))) {
                return SLOW_QUERY_LOG;
            }
        }
        return OTHER;
    }

    // enable/disable @@STATISTIC
    private static int stCheck(String stmt, int offset) {
        if (stmt.length() > offset + 7) {
            String prefix = stmt.substring(offset).toUpperCase();
            if (prefix.startsWith("TATISTIC") && (stmt.length() == offset + 8 || ParseUtil.isEOF(stmt, offset + 8))) {
                return STATISTIC;
            }

        }
        return OTHER;
    }

    // enable/disable @@SQLDUMP_SQL
    private static int sqCheck(String stmt, int offset) {
        if (stmt.length() > offset + 9) {
            String prefix = stmt.substring(offset).toUpperCase();
            if (prefix.startsWith("QLDUMP_SQL") && (stmt.length() == offset + 10 || ParseUtil.isEOF(stmt, offset + 10))) {
                return SQLDUMP_SQL;
            }
        }
        return OTHER;
    }

    private static int gCheck(String stmt, int offset) {
        if (stmt.length() > offset + 10) {
            String prefix = stmt.substring(offset).toUpperCase();
            if (prefix.startsWith("GENERAL_LOG") && (stmt.length() == offset + 11 || ParseUtil.isEOF(stmt, offset + 11))) {
                return GENERAL_LOG;
            }
        }
        return OTHER;
    }


    private static int cCheck(String stmt, int offset) {
        if (stmt.length() > offset + 20) {
            // enable/disable @@cap_client_found_rows
            String prefix = stmt.substring(offset).toUpperCase();
            if (prefix.startsWith("CAP_CLIENT_FOUND_ROWS") && (stmt.length() == offset + 21 || ParseUtil.isEOF(stmt, offset + 21))) {
                return CAP_CLIENT_FOUND_ROWS;
            }
        } else if (stmt.length() > offset + 14) {
            // enable/disable @@custom_mysql_ha
            String prefix = stmt.substring(offset).toUpperCase();
            if (prefix.startsWith("CUSTOM_MYSQL_HA") && (stmt.length() == offset + 15 || ParseUtil.isEOF(stmt, offset + 15))) {
                return CUSTOM_MYSQL_HA;
            }
        }
        return OTHER;
    }
}
