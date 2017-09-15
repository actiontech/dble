/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.response.ShowColumns;
import com.actiontech.dble.server.response.ShowCreateStmtInfo;
import com.actiontech.dble.server.response.ShowIndex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mycat
 */
public final class ServerParseShow {
    private ServerParseShow() {
    }

    public static final int OTHER = -1;
    public static final int DATABASES = 1;
    public static final int TABLES = 5;
    public static final int CHARSET = 7;
    public static final int COLUMNS = 8;
    public static final int INDEX = 9;
    public static final int CREATE_TABLE = 10;

    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
                case ' ':
                case '\r':
                case '\n':
                case '\t':
                    continue;
                case 'F':
                case 'f':
                    return showFCheck(stmt, i);
                case '/':
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    continue;
                case 'D':
                case 'd':
                    return dataCheck(stmt, i);
                case 'T':
                case 't':
                    return showTableType(stmt);
                case 'S':
                case 's':
                    return schemasCheck(stmt, i);
                case 'C':
                case 'c':
                    return showCCheck(stmt, i);
                case 'I':
                case 'i':
                    return showIndex(stmt);
                case 'K':
                case 'k':
                    return showIndex(stmt);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int showCCheck(String stmt, int offset) {
        if (stmt.length() > offset++) {
            if (stmt.charAt(offset) == 'h' || stmt.charAt(offset) == 'H') {
                return charsetCheck(stmt, offset);
            } else if (stmt.charAt(offset) == 'o' || stmt.charAt(offset) == 'O') {
                return showColumns(stmt);
            } else if (stmt.charAt(offset) == 'r' || stmt.charAt(offset) == 'R') {
                return showCreateTable(stmt, offset);
            } else {
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int showFCheck(String stmt, int offset) {
        if (stmt.length() > offset++) {
            if (stmt.charAt(offset) == 'u' || stmt.charAt(offset) == 'U') {
                return showFullCheck(stmt, offset);
            } else if (stmt.charAt(offset) == 'i' || stmt.charAt(offset) == 'I') { //show fields
                return showColumns(stmt);
            } else {
                return OTHER;
            }
        }
        return OTHER;
    }

    private static int showFullCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ll ?".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') &&
                    (c2 == 'L' || c2 == 'l') && ParseUtil.isSpace(stmt.charAt(++offset))) {
                while (stmt.length() > ++offset) {
                    if (ParseUtil.isSpace(stmt.charAt(offset))) {
                        continue;
                    }
                    switch (stmt.charAt(offset)) {
                        case 'T':
                        case 't':
                            return showTableType(stmt);
                        case 'C':
                        case 'c':
                            return showColumns(stmt);
                        default:
                            return OTHER;
                    }
                }
            }
        }
        return OTHER;
    }

    // SHOW DATA
    private static int dataCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ata?".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'T' || c2 == 't') && (c3 == 'A' || c3 == 'a')) {
                switch (stmt.charAt(++offset)) {
                    case 'B':
                    case 'b':
                        return showDatabases(stmt, offset);
                    default:
                        return OTHER;
                }
            }
        }
        return OTHER;
    }

    //show create table
    private static int showCreateTable(String stmt, int offset) {
        if (stmt.length() > offset + "eate".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') &&
                    (c2 == 'A' || c2 == 'a') &&
                    (c3 == 'T' || c3 == 't') &&
                    (c4 == 'E' || c4 == 'e')) {
                while (stmt.length() > ++offset) {
                    if (ParseUtil.isSpace(stmt.charAt(offset))) {
                        continue;
                    }
                    switch (stmt.charAt(offset)) {
                        case 'T':
                        case 't':
                            char c5 = stmt.charAt(++offset);
                            char c6 = stmt.charAt(++offset);
                            char c7 = stmt.charAt(++offset);
                            char c8 = stmt.charAt(++offset);
                            if ((c5 == 'A' || c5 == 'a') &&
                                    (c6 == 'B' || c6 == 'b') &&
                                    (c7 == 'L' || c7 == 'l') &&
                                    (c8 == 'E' || c8 == 'e') &&
                                    (ParseUtil.isSpace(stmt.charAt(++offset)))) {
                                return CREATE_TABLE;
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

    // SHOW DATABASES
    private static int showDatabases(String stmt, int offset) {
        if (stmt.length() > offset + "ases".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') &&
                    (c2 == 'S' || c2 == 's') &&
                    (c3 == 'E' || c3 == 'e') &&
                    (c4 == 'S' || c4 == 's') &&
                    (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset))) {
                return DATABASES;
            }
        }
        return OTHER;
    }

    //show schemas
    private static int schemasCheck(String stmt, int offset) {
        if (stmt.length() > offset + "chemas".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'C' || c1 == 'c') &&
                    (c2 == 'H' || c2 == 'h') &&
                    (c3 == 'E' || c3 == 'e') &&
                    (c4 == 'M' || c4 == 'm') &&
                    (c5 == 'A' || c5 == 'a') &&
                    (c6 == 'S' || c6 == 's') &&
                    (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset))) {
                return DATABASES;
            }
        }
        return OTHER;
    }

    //show charset
    private static int charsetCheck(String stmt, int offset) {
        if (stmt.length() > offset + "arset".length()) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c2 == 'A' || c2 == 'a') &&
                    (c3 == 'R' || c3 == 'r') &&
                    (c4 == 'S' || c4 == 's') &&
                    (c5 == 'E' || c5 == 'e') &&
                    (c6 == 'T' || c6 == 't') &&
                    (stmt.length() == ++offset || ParseUtil.isEOF(stmt, offset))) {
                return CHARSET;
            }
        }
        return OTHER;
    }

    public static int showTableType(String sql) {
        Pattern pattern = ShowCreateStmtInfo.PATTERN;
        Matcher ma = pattern.matcher(sql);
        if (ma.matches()) {
            return TABLES;
        } else {
            return OTHER;
        }
    }

    private static int showColumns(String sql) {
        Pattern pattern = ShowColumns.PATTERN;
        Matcher ma = pattern.matcher(sql);
        if (ma.matches()) {
            return COLUMNS;
        } else {
            return OTHER;
        }
    }

    private static int showIndex(String sql) {
        Pattern pattern = ShowIndex.PATTERN;
        Matcher ma = pattern.matcher(sql);
        if (ma.matches()) {
            return INDEX;
        } else {
            return OTHER;
        }
    }
}
