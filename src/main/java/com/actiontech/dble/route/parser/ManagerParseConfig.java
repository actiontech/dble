/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

public final class ManagerParseConfig {
    public static final int OTHER = -1;
    public static final int CONFIG = 1;
    public static final int CONFIG_ALL = 2;

    private int configAllMode;
    public static final int OPTS_MODE = 1;
    public static final int OPTF_MODE = 2;
    public static final int OPTR_MODE = 4;

    public ManagerParseConfig() {
        configAllMode = 0;
    }

    public int getMode() {
        return configAllMode;
    }

    public int parse(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            switch (stmt.charAt(++offset)) {
                case '_':
                case '-':
                    return checkConfigAll(stmt, offset);
                default:
                    offset = skipSpaceAndComment(stmt, offset);
                    if (stmt.length() == ++offset)
                        return CONFIG;
                    else
                        return OTHER;
            }
        }
        return CONFIG;
    }

    private int checkConfigAll(String stmt, int offset) {
        if (stmt.length() > offset + 2) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'a' || c1 == 'A') && (c2 == 'l' || c2 == 'L') && (c3 == 'l' || c3 == 'L')) {
                if (stmt.length() > offset + 1) {
                    char c4 = stmt.charAt(++offset);
                    if (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n' || c4 == '/' || c4 == '#') {
                        offset = skipSpaceAndComment(stmt, offset);
                        if (stmt.length() == offset + 1)
                            return CONFIG_ALL;

                        return checkAllOpt(stmt, offset);
                    }
                } else {
                    return CONFIG_ALL;
                }
            }
        }
        return OTHER;
    }

    private int checkAllOpt(String stmt, int offset) {
        char c1 = stmt.charAt(++offset);
        if (c1 == '-') {
            if (stmt.length() > offset + 1) {
                offset = checkSeriesOpt(stmt, offset);
                if (stmt.length() > offset + 1) {
                    char c2 = stmt.charAt(++offset);
                    if (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n' || c2 == '/' || c2 == '#') {
                        offset = skipSpaceAndComment(stmt, offset);
                        if (stmt.length() == offset + 1)
                            return CONFIG_ALL;

                        return checkAllOpt(stmt, offset);
                    } else {
                        /* -t/-f xxx */
                        return OTHER;
                    }
                }
                return CONFIG_ALL;
            }
            return OTHER;
        }
        return OTHER;
    }

    private int checkSeriesOpt(String stmt, int offset) {
        for (++offset; offset < stmt.length(); ++offset) {
            char c1 = stmt.charAt(offset);
            if (c1 == 's' || c1 == 'S') {
                configAllMode = configAllMode | OPTS_MODE;
            } else if (c1 == 'f' || c1 == 'F') {
                configAllMode = configAllMode | OPTF_MODE;
            } else if (c1 == 'r' || c1 == 'R') {
                configAllMode = configAllMode | OPTR_MODE;
            } else {
                return offset - 1;
            }
        }
        return offset - 1;
    }

    private int skipSpaceAndComment(String stmt, int offset) {
        for (; offset < stmt.length(); ++offset) {
            switch (stmt.charAt(offset)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
                case '/':
                case '#':
                    offset = ParseUtil.comment(stmt, offset);
                    continue;
                default:
                    return offset - 1;
            }
        }
        return offset - 1;
    }
}
