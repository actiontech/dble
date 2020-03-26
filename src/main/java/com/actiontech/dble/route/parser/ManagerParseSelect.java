/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.route.parser;

import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.route.parser.util.ParseUtil;


public final class ManagerParseSelect {
    private ManagerParseSelect() {
    }

    public static final int OTHER = -1;
    public static final int VERSION_COMMENT = 1;
    public static final int SESSION_TX_READ_ONLY = 2;
    public static final int MAX_ALLOWED_PACKET = 3;
    public static final int TIMEDIFF = 4;
    public static final int SESSION_TRANSACTION_READ_ONLY = 5;

    private static final char[] STRING_VERSION_COMMENT = "VERSION_COMMENT".toCharArray();
    private static final char[] STRING_SESSION = "SESSION.".toCharArray();
    private static final char[] STRING_SESSION_TRANSACTION_READ_ONLY = VersionUtil.TRANSACTION_READ_ONLY.toUpperCase().toCharArray();
    private static final char[] STRING_SESSION_TX_READ_ONLY = VersionUtil.TX_READ_ONLY.toUpperCase().toCharArray();
    private static final char[] STRING_MAX_ALLOWED_PACKET = "MAX_ALLOWED_PACKET".toCharArray();
    private static final char[] STRING_TIMEDIFF = "TIMEDIFF(NOW(), UTC_TIMESTAMP())".toCharArray();

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
                    return selectTCheck(stmt, i);
                case '@':
                    return select2Check(stmt, i);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int selectTCheck(String stmt, int offset) {
        int length = offset + STRING_TIMEDIFF.length;
        if (stmt.length() >= length && ParseUtil.compare(stmt, offset, STRING_TIMEDIFF)) {
            if (stmt.length() > length && stmt.charAt(length) != ' ') {
                return OTHER;
            }
            return (offset << 8) | TIMEDIFF;
        }
        return OTHER;
    }

    private static int select2Check(String stmt, int offset) {
        if (stmt.length() > ++offset && stmt.charAt(offset) == '@' && stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'S':
                case 's':
                    return select2SCheck(stmt, offset);
                case 'V':
                case 'v':
                    return select2VCheck(stmt, offset);
                case 'M':
                case 'm':
                    return select2MCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int select2MCheck(String stmt, int offset) {
        int length = offset + STRING_MAX_ALLOWED_PACKET.length;
        if (stmt.length() >= length && ParseUtil.compare(stmt, offset, STRING_MAX_ALLOWED_PACKET)) {
            if (stmt.length() > length && stmt.charAt(length) != ' ') {
                return OTHER;
            }
            return MAX_ALLOWED_PACKET;
        }
        return OTHER;
    }

    // VERSION_COMMENT
    private static int select2VCheck(String stmt, int offset) {
        int length = offset + STRING_VERSION_COMMENT.length;
        if (stmt.length() >= length && ParseUtil.compare(stmt, offset, STRING_VERSION_COMMENT)) {
            if (stmt.length() > length && stmt.charAt(length) != ' ') {
                return OTHER;
            }
            return VERSION_COMMENT;
        }
        return OTHER;
    }

    // SESSION.
    private static int select2SCheck(String stmt, int offset) {
        int length = offset + STRING_SESSION.length;
        if (stmt.length() < length + 2) { //session.tx
            return OTHER;
        }
        if (ParseUtil.compare(stmt, offset, STRING_SESSION)) {
            if (ParseUtil.isEOF(stmt, length)) {
                return OTHER;
            }
        } else {
            return OTHER;
        }
        offset = offset + STRING_SESSION.length;
        switch (stmt.charAt(offset)) {
            case 'T':
            case 't':
                switch (stmt.charAt(offset + 1)) {
                    case 'X':
                    case 'x':
                        return select2TXCheck(stmt, offset);
                    case 'R':
                    case 'r':
                        return select2TRCheck(stmt, offset);
                    default:
                        return OTHER;
                }
            default:
                return OTHER;
        }
    }

    // SESSION.TX_READ_ONLY
    private static int select2TXCheck(String stmt, int offset) {
        int length = offset + STRING_SESSION_TX_READ_ONLY.length;
        if (stmt.length() >= length && ParseUtil.compare(stmt, offset, STRING_SESSION_TX_READ_ONLY)) {
            if (!ParseUtil.isEOF(stmt, length)) {
                return OTHER;
            }
            return SESSION_TX_READ_ONLY;
        }

        return OTHER;
    }

    // SESSION.TRANSACTION_READ_ONLY
    private static int select2TRCheck(String stmt, int offset) {
        int length = offset + STRING_SESSION_TRANSACTION_READ_ONLY.length;
        if (stmt.length() >= length && ParseUtil.compare(stmt, offset, STRING_SESSION_TRANSACTION_READ_ONLY)) {
            if (!ParseUtil.isEOF(stmt, length)) {
                return OTHER;
            }
            return SESSION_TRANSACTION_READ_ONLY;
        }

        return OTHER;
    }

}
