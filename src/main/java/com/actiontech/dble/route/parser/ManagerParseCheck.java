/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.route.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ManagerParseCheck {
    private ManagerParseCheck() {
    }

    public static final int OTHER = -1;
    public static final int META_DATA = 1;
    public static final int FULL_META_DATA = 2;
    public static final int GLOBAL_CONSISTENCY = 3;

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
                case 'F':
                case 'f':
                    return check2FCheck(stmt, offset);
                case '@':
                    return check2Check(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int check2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@' && stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'M':
                case 'm':
                    return check2MCheck(stmt, offset);
                case 'G':
                case 'g':
                    return check2GCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // CHECK FULL @@METADATA
    private static int check2FCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'U' || c1 == 'u') && (c2 == 'L' || c2 == 'l') && (c3 == 'L' || c3 == 'l') && ParseUtil.isSpace(c4)) {
                int i = offset;
                for (; i < stmt.length(); i++) {
                    switch (stmt.charAt(i)) {
                        case ' ':
                            continue;
                        case '/':
                        case '@':
                            int res = check2Check(stmt, i);
                            if (res == META_DATA) {
                                return FULL_META_DATA;
                            }
                            return OTHER;
                        default:
                            return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    // CHECK @@METADATA
    private static int check2MCheck(String stmt, int offset) {
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
                return META_DATA;
            }
        }
        return OTHER;
    }

    // CHECK @@GLOBAL
    private static int check2GCheck(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') &&
                    (c2 == 'O' || c2 == 'o') &&
                    (c3 == 'B' || c3 == 'b') &&
                    (c4 == 'A' || c4 == 'a') &&
                    (c5 == 'L' || c5 == 'l')) {
                return GLOBAL_CONSISTENCY;
            }
        }
        return OTHER;
    }
}

