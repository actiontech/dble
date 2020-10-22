/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public class ServerParseStart {
    protected ServerParseStart() {
    }

    public static final int OTHER = -1;
    public static final int TRANSACTION = 1;
    public static final int READCHARCS = 2;

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
                case 'T':
                case 't':
                    return transactionCheck(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // transaction characteristic check
    private static int transactionCheck(String stmt, int offset) {
        int tmpOff;
        tmpOff = skipTrans(stmt, offset);
        if (tmpOff < 0) {
            return OTHER;
        }

        if (stmt.length() == ++tmpOff) {
            return TRANSACTION;
        } else {
            return readCharcsCheck(stmt, tmpOff);
        }
    }

    protected static int skipTrans(String stmt, int offset) {
        if (stmt.length() > offset + "ransaction".length()) {
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
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'A' || c2 == 'a') && (c3 == 'N' || c3 == 'n') &&
                    (c4 == 'S' || c4 == 's') && (c5 == 'A' || c5 == 'a') && (c6 == 'C' || c6 == 'c') &&
                    (c7 == 'T' || c7 == 't') && (c8 == 'I' || c8 == 'i') && (c9 == 'O' || c9 == 'o') &&
                    (c10 == 'N' || c10 == 'n')) {
                return offset;
            }
        }

        return -1;
    }

    static int readCharcsCheck(String stmt, int offset) {
        int currentOffset = ParseUtil.skipSpace(stmt, offset);
        if (stmt.length() == currentOffset) {
            return TRANSACTION;
        }

        currentOffset = ParseUtil.commentHint(stmt, currentOffset);
        if (stmt.length() == ++currentOffset || ParseUtil.skipSpace(stmt, currentOffset) == stmt.length()) {
            return TRANSACTION;
        }

        if (stmt.length() > offset + "ead ".length()) {
            char c0 = stmt.charAt(offset);
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c0 == 'R' || c0 == 'r') && (c1 == 'E' || c1 == 'e') && (c2 == 'A' || c2 == 'a') &&
                    (c3 == 'D' || c3 == 'd') && (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                return READCHARCS;
            }
        }

        return OTHER;
    }
}
