/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.parser;

import com.actiontech.dble.route.parser.util.ParseUtil;

/**
 * @author mycat
 */
public final class ServerParseSet {
    private ServerParseSet() {
    }

    public static final int SYNTAX_ERROR = -99;
    public static final int TX_WITHOUT_KEYWORD = -3;
    public static final int GLOBAL = -2;
    //TODO: DELETE OTHER
    public static final int OTHER = -1;
    public static final int AUTOCOMMIT_ON = 1;
    public static final int AUTOCOMMIT_OFF = 2;
    public static final int TX_READ_UNCOMMITTED = 3;
    public static final int TX_READ_COMMITTED = 4;
    public static final int TX_REPEATED_READ = 5;
    public static final int TX_SERIALIZABLE = 6;
    public static final int NAMES = 7;
    public static final int CHARACTER_SET_CLIENT = 8;
    public static final int CHARACTER_SET_CONNECTION = 9;
    public static final int CHARACTER_SET_RESULTS = 10;
    public static final int XA_FLAG_ON = 11;
    public static final int XA_FLAG_OFF = 12;
    public static final int CHARACTER_SET_NAME = 13;
    public static final int TX_READ_WRITE = 14;
    public static final int TX_READ_ONLY = 15;
    public static final int COLLATION_CONNECTION = 16;

    public static final int SYSTEM_VARIABLES = 97;
    public static final int USER_VARIABLES = 98;
    public static final int MULTI_SET = 99;

    private static final int VALUE_ON = 1;
    private static final int VALUE_OFF = 0;

    public static int parse(String stmt, int offset) {
        while (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case ' ':
                case '\r':
                case '\n':
                case '\t':
                    continue;
                case '/':
                case '#':
                    //TODO:DELETE COMMENT?
                    offset = ParseUtil.comment(stmt, offset);
                    continue;
                case 'C':
                case 'c':
                    return checkC(stmt, offset);
                case 'N':
                case 'n':
                    return names(stmt, offset);
                case 'X':
                case 'x':
                    return xaFlag(stmt, offset);
                case 'A':
                case 'a':
                    return autocommit(stmt, offset);
                case 'T':
                case 't':
                    int res = parseT(stmt, offset);
                    if (res != SYNTAX_ERROR && res != SYSTEM_VARIABLES) {
                        res = TX_WITHOUT_KEYWORD;
                    }
                    return res;
                case 'S':
                case 's':
                    return session(stmt, offset);
                case 'G':
                case 'g':
                    return global(stmt, offset);
                case '@':
                    return parseAt(stmt, offset);
                default:
                    return checkSystemVariables(stmt);
            }
        }
        return SYNTAX_ERROR;
    }

    // set xa=1
    private static int xaFlag(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            char c1 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a')) {
                return checkSwitchForExpect(stmt, offset, XA_FLAG_ON, XA_FLAG_OFF);
            }
        }
        return checkSystemVariables(stmt);
    }

    //AUTOCOMMIT(' '=)
    private static int autocommit(String stmt, int offset) {
        if (stmt.length() > offset + 9) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            if ((c1 == 'U' || c1 == 'u') && (c2 == 'T' || c2 == 't') &&
                    (c3 == 'O' || c3 == 'o') && (c4 == 'C' || c4 == 'c') &&
                    (c5 == 'O' || c5 == 'o') && (c6 == 'M' || c6 == 'm') &&
                    (c7 == 'M' || c7 == 'm') && (c8 == 'I' || c8 == 'i') &&
                    (c9 == 'T' || c9 == 't')) {
                return checkSwitchForExpect(stmt, offset, AUTOCOMMIT_ON, AUTOCOMMIT_OFF);
            }
        }
        return checkSystemVariables(stmt);
    }

    // check value is "=on/off/0/1"
    private static int checkSwitchForExpect(String stmt, int offset, int expectOn, int expectOff) {
        while (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case ' ':
                case '\r':
                case '\n':
                case '\t':
                    continue;
                case '=':
                    int value = parserSwitchValue(stmt, offset);
                    if (value == VALUE_ON) {
                        return expectOn;
                    } else if (value == VALUE_OFF) {
                        return expectOff;
                    } else {
                        return value;
                    }
                default:
                    return SYNTAX_ERROR;
            }
        }
        return SYNTAX_ERROR;
    }

    private static int parserSwitchValue(String stmt, int offset) {
        while (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case ' ':
                case '\r':
                case '\n':
                case '\t':
                    continue;
                case '1':
                    return parseValueForExpected(stmt, offset, VALUE_ON);
                case '0':
                    return parseValueForExpected(stmt, offset, VALUE_OFF);
                case 'O':
                case 'o':
                    return parseValueO(stmt, offset);
                default:
                    return SYNTAX_ERROR;
            }
        }
        return SYNTAX_ERROR;
    }

    private static int parseValueForExpected(String stmt, int offset, int expectValue) {
        if (stmt.length() == ++offset) {
            return expectValue;
        }
        int pos = offset;
        if (ParseUtil.isEOF(stmt, pos)) {
            return expectValue;
        } else if (isMultiSet(stmt, pos)) {
            return MULTI_SET;
        } else {
            return SYNTAX_ERROR;
        }
    }

    private static boolean isMultiSet(String stmt, int offset) {
        for (; offset < stmt.length(); offset++) {
            switch (stmt.charAt(offset)) {
                case ' ':
                case '\r':
                case '\n':
                case '\t':
                    continue;
                case ',':
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }

    //ON/OFF
    private static int parseValueO(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'N':
                case 'n':
                    return parseValueForExpected(stmt, offset, VALUE_ON);
                case 'F':
                case 'f':
                    return parseValueOff(stmt, offset);
                default:
                    return SYNTAX_ERROR;
            }
        }
        return SYNTAX_ERROR;
    }

    // SET AUTOCOMMIT = OFF
    private static int parseValueOff(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'F':
                case 'f':
                    return parseValueForExpected(stmt, offset, VALUE_OFF);
                default:
                    return SYNTAX_ERROR;
            }
        }
        return SYNTAX_ERROR;
    }

    // SET NAMES' '
    private static int names(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'M' || c2 == 'm') &&
                    (c3 == 'E' || c3 == 'e') && (c4 == 'S' || c4 == 's') &&
                    ParseUtil.isSpace(c5)) {
                if (stmt.indexOf(',', offset) >= 0) {
                    return MULTI_SET;
                } else {
                    return (offset << 8) | NAMES;
                }
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET C
    private static int checkC(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            char c1 = stmt.charAt(++offset);
            if ((c1 == 'H' || c1 == 'h')) {
                return checkChar(stmt, offset);
            }
            if ((c1 == 'O' || c1 == 'o')) {
                return checkCollation(stmt, offset);
            } else {
                return checkSystemVariables(stmt);
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET Collation_connection
    private static int checkCollation(String stmt, int offset) {
        if (stmt.substring(offset).toLowerCase().startsWith("ollation_connection")) {
            return checkEqualFormat(stmt, offset + 19, COLLATION_CONNECTION);
        }
        return checkSystemVariables(stmt);
    }

    // SET CHAR
    private static int checkChar(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c3 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c3 == 'A' || c3 == 'a') && (c2 == 'R' || c2 == 'r')) {
                switch (c4) {
                    case 'a':
                    case 'A':
                        return character(stmt, offset);
                    case 'S':
                    case 's':
                        return checkCharSet(stmt, offset);
                    default:
                        return checkSystemVariables(stmt);
                }
            } else {
                return checkSystemVariables(stmt);
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET CHARACTER
    private static int character(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'C' || c1 == 'c') && (c2 == 'T' || c2 == 't') &&
                    (c3 == 'E' || c3 == 'e') && (c4 == 'R' || c4 == 'r')) {
                switch (c5) {
                    case ' ':
                    case '\r':
                    case '\n':
                    case '\t':
                        return characterS(stmt, offset);
                    case '_':
                        return characterSetDetail(stmt, offset);
                    default:
                        return checkSystemVariables(stmt);
                }
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET CHARACTER S
    private static int characterS(String stmt, int offset) {
        while (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case ' ':
                case '\r':
                case '\n':
                case '\t':
                    continue;
                case 'S':
                case 's':
                    return checkCharSet(stmt, offset);
                default:
                    return checkSystemVariables(stmt);
            }
        }
        return checkSystemVariables(stmt);
    }

    //SET {CHARACTER SET | CHARSET} _
    private static int checkCharSet(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'T' || c2 == 't') && ParseUtil.isSpace(c3)) {
                if (stmt.indexOf(',', offset) >= 0) {
                    return MULTI_SET;
                } else {
                    return (offset << 8) | CHARACTER_SET_NAME;
                }
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET CHARACTER_SET_
    private static int characterSetDetail(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'S' || c1 == 's') && (c2 == 'E' || c2 == 'e') &&
                    (c3 == 'T' || c3 == 't') && (c4 == '_')) {
                switch (c5) {
                    case 'R':
                    case 'r':
                        return characterSetResults(stmt, offset);
                    case 'C':
                    case 'c':
                        return characterSetC(stmt, offset);
                    default:
                        return checkSystemVariables(stmt);
                }
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET CHARACTER_SET_RESULTS =
    private static int characterSetResults(String stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'S' || c2 == 's') && (c3 == 'U' || c3 == 'u') &&
                    (c4 == 'L' || c4 == 'l') && (c5 == 'T' || c5 == 't') && (c6 == 'S' || c6 == 's')) {
                return checkEqualFormat(stmt, offset, CHARACTER_SET_RESULTS);
            }
        }
        return checkSystemVariables(stmt);
    }

    private static int checkEqualFormat(String stmt, int offset, int expectValue) {
        while (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case ' ':
                case '\r':
                case '\n':
                case '\t':
                    continue;
                case '=':
                    while (stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                            case ' ':
                            case '\r':
                            case '\n':
                            case '\t':
                                continue;
                            default:
                                if (stmt.indexOf(',', offset) >= 0) {
                                    return MULTI_SET;
                                } else {
                                    return (offset << 8) | expectValue;
                                }
                        }
                    }
                    return checkSystemVariables(stmt);
                default:
                    return checkSystemVariables(stmt);
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET CHARACTER_SET_C
    private static int characterSetC(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            char c1 = stmt.charAt(++offset);
            switch (c1) {
                case 'o':
                case 'O':
                    return characterSetConnection(stmt, offset);
                case 'l':
                case 'L':
                    return characterSetClient(stmt, offset);
                default:
                    return checkSystemVariables(stmt);
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET CHARACTER_SET_CONNECTION =
    private static int characterSetConnection(String stmt, int offset) {
        if (stmt.length() > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'N' || c2 == 'n') &&
                    (c3 == 'E' || c3 == 'e') && (c4 == 'C' || c4 == 'c') &&
                    (c5 == 'T' || c5 == 't') && (c6 == 'I' || c6 == 'i') &&
                    (c7 == 'O' || c7 == 'o') && (c8 == 'N' || c8 == 'n')) {
                return checkEqualFormat(stmt, offset, CHARACTER_SET_CONNECTION);
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET CHARACTER_SET_CLIENT =
    private static int characterSetClient(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'I' || c1 == 'i') && (c2 == 'E' || c2 == 'e') &&
                    (c3 == 'N' || c3 == 'n') && (c4 == 'T' || c4 == 't')) {
                return checkEqualFormat(stmt, offset, CHARACTER_SET_CLIENT);
            }
        }
        return checkSystemVariables(stmt);
    }

    //GLOBAL +space
    private static int global(String stmt, int offset) {
        if (stmt.length() > offset + 6) {
            if (isGlobal(stmt, offset)) {
                offset = offset + 5;
                if (!ParseUtil.isSpace(stmt.charAt(++offset))) {
                    return SYNTAX_ERROR; // SE GLOBAL(NO WHITESPACE)
                }
                return GLOBAL;
            }
        }
        return checkSystemVariables(stmt);
    }

    //SESSION +space
    private static int session(String stmt, int offset) {
        if (stmt.length() > offset + 7) {
            if (isSession(stmt, offset)) {
                offset = offset + 6;
                if (!ParseUtil.isSpace(stmt.charAt(++offset))) {
                    return checkSystemVariables(stmt);
                }
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'T':
                        case 't':
                            return parseT(stmt, offset);
                        case 'A':
                        case 'a':
                            return autocommit(stmt, offset);
                        default:
                            return checkSystemVariables(stmt);
                    }
                }
            }
        }
        return checkSystemVariables(stmt);
    }

    // set @@
    private static int parseAt(String stmt, int offset) {
        if (stmt.length() <= offset + 2) {
            return SYNTAX_ERROR; // SET @;||SET @X;
        }
        if (stmt.charAt(++offset) != '@') {
            return checkUserVariables(stmt);
        }
        //System variables
        switch (stmt.charAt(++offset)) {
            case 'S':
            case 's':
                return sessionDot(stmt, offset);
            case 'A':
            case 'a':
                return autocommit(stmt, offset);
            case 'G':
            case 'g':
                return globalDot(stmt, offset);
            case 'T':
            case 't':
                int res = parseAtT(stmt, offset);
                if (res != SYNTAX_ERROR && res != SYSTEM_VARIABLES) {
                    res = TX_WITHOUT_KEYWORD;
                }
                return res;
            default:
                return checkSystemVariables(stmt);
        }
    }

    // SET @@SESSION.
    private static int sessionDot(String stmt, int offset) {
        if (stmt.length() > offset + 8) {
            if (isSession(stmt, offset)) {
                offset = offset + 6;
                if (stmt.charAt(++offset) == '.') {
                    switch (stmt.charAt(++offset)) {
                        case 'T':
                        case 't':
                            return parseAtT(stmt, offset);
                        case 'A':
                        case 'a':
                            return autocommit(stmt, offset);
                        default:
                            return checkSystemVariables(stmt);
                    }
                } else {
                    return SYNTAX_ERROR;
                }
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET @@GLOBAL.
    private static int globalDot(String stmt, int offset) {
        if (stmt.length() > offset + 6) {
            if (isGlobal(stmt, offset)) {
                offset = offset + 5;
                if (stmt.charAt(++offset) == '.') {
                    return GLOBAL;
                } else {
                    return SYNTAX_ERROR;
                }
            }
        }
        return checkSystemVariables(stmt);
    }

    // @@session.tx_ || @@session.transaction_ || @@x_ || @@transaction_
    private static int parseAtT(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            switch (stmt.charAt(++offset)) {
                case 'x':
                case 'X':
                    return parseTx(stmt, offset);
                case 'r':
                case 'R':
                    return parseTransaction(stmt, offset);
                default:
                    return checkSystemVariables(stmt);
            }
        }
        return SYNTAX_ERROR; //SET @@[SESSION.]T
    }

    private static int parseTransaction(String stmt, int offset) {
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
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'N' || c2 == 'n') &&
                    (c3 == 'S' || c3 == 's') && (c4 == 'A' || c4 == 'a') && (c5 == 'C' || c5 == 'c') &&
                    (c6 == 'T' || c6 == 't') && (c7 == 'I' || c7 == 'i') && (c8 == 'O' || c8 == 'o') &&
                    (c9 == 'N' || c9 == 'n') && (c10 == '_')) {
                return parseTxContent(stmt, offset);
            }
        }
        return checkSystemVariables(stmt);
    }

    private static int parseTx(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            if ((stmt.charAt(++offset) == '_')) {
                return parseTxContent(stmt, offset);
            }
        }
        return checkSystemVariables(stmt);
    }

    private static int parseTxContent(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            switch (stmt.charAt(++offset)) {
                case 'i':
                case 'I':
                    return checkIsolationVariables(stmt, offset);
                case 'r':
                case 'R':
                    return checkReadOnlyVariables(stmt, offset);
                default:
                    return checkSystemVariables(stmt);
            }
        }
        return SYNTAX_ERROR;
    }

    private static int checkReadOnlyVariables(String stmt, int offset) {
        if (stmt.length() > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'A' || c2 == 'a') && (c3 == 'D' || c3 == 'd') &&
                    (c4 == '_') && (c5 == 'O' || c5 == 'o') && isOnly(stmt, offset)) {
                offset = offset + 3;
                return checkSwitchForExpect(stmt, offset, TX_READ_ONLY, TX_READ_WRITE);
            }
        }
        return checkSystemVariables(stmt);
    }

    private static int checkIsolationVariables(String stmt, int offset) {
        if (stmt.length() > offset + 8 && isIsolation(stmt, offset)) {
            offset = offset + 8;
            while (stmt.length() > ++offset) {
                switch (stmt.charAt(offset)) {
                    case ' ':
                    case '\r':
                    case '\n':
                    case '\t':
                        continue;
                    case '=':
                        return parserIsolationValue(stmt, offset);
                    default:
                        return SYNTAX_ERROR;
                }
            }
        }
        return checkSystemVariables(stmt);
    }

    private static int parserIsolationValue(String stmt, int offset) {
        while (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case ' ':
                case '\r':
                case '\n':
                case '\t':
                    continue;
                case '\'':
                case '`':
                    return parserLevel(stmt, offset + 1, true, stmt.charAt(offset));
                case 'S':
                case 's':
                    return serializable(stmt, offset, false, ' ');
                default:
                    return SYNTAX_ERROR;
            }
        }
        return SYNTAX_ERROR;
    }

    private static int parseT(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            switch (stmt.charAt(++offset)) {
                case 'x':
                case 'X':
                    return parseTx(stmt, offset);
                case 'r':
                case 'R':
                    return transaction(stmt, offset);
                default:
                    return checkSystemVariables(stmt);
            }
        }
        return SYNTAX_ERROR; //SET @@[SESSION.]T
    }
    // SET [SESSION] TRANSACTION ISOLATION LEVEL||SET [SESSION] TRANSACTION_ISOLATION =
    private static int transaction(String stmt, int offset) {
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
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'N' || c2 == 'n') &&
                    (c3 == 'S' || c3 == 's') && (c4 == 'A' || c4 == 'a') && (c5 == 'C' || c5 == 'c') &&
                    (c6 == 'T' || c6 == 't') && (c7 == 'I' || c7 == 'i') && (c8 == 'O' || c8 == 'o') &&
                    (c9 == 'N' || c9 == 'n')) {
                char flag = stmt.charAt(++offset);
                boolean horizontal;
                if (ParseUtil.isSpace(flag)) {
                    horizontal = false;
                } else if (flag == '_') {
                    horizontal = true;
                } else {
                    return checkSystemVariables(stmt);
                }
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'I':
                        case 'i':
                            if (horizontal) {
                                return checkIsolationVariables(stmt, offset);
                            }
                            return isolation(stmt, offset);
                        case 'R':
                        case 'r':
                            return readOnlyOrWrite(stmt, offset, horizontal);
                        default:
                            return SYNTAX_ERROR;
                    }
                }
            }
        }
        return checkSystemVariables(stmt);
    }

    // SET SESSION TRANSACTION ISOLATION  , if child check failed ,return SYNTAX_ERROR
    private static int isolation(String stmt, int offset) {
        if (stmt.length() > offset + 9) {
            if (isIsolation(stmt, offset)) {
                offset = offset + 8;
                if (!ParseUtil.isSpace(stmt.charAt(++offset))) {
                    return checkSystemVariables(stmt);
                }
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'L':
                        case 'l':
                            return level(stmt, offset);
                        default:
                            return SYNTAX_ERROR;
                    }
                }
            }
        }
        return SYNTAX_ERROR;
    }

    // SET SESSION TRANSACTION READ ONLY/WRITE
    private static int readOnlyOrWrite(String stmt, int offset, boolean horizontal) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'A' || c2 == 'a') && (c3 == 'D' || c3 == 'd')) {
                char flag = stmt.charAt(++offset);
                if ((horizontal && flag != '_') || (!horizontal && !ParseUtil.isSpace(flag))) {
                    return SYNTAX_ERROR;
                }
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'O':
                        case 'o':
                            if (isOnly(stmt, offset)) {
                                if (horizontal) {
                                    return checkSwitchForExpect(stmt, offset + 4, TX_READ_ONLY, TX_READ_WRITE);
                                } else if (!horizontal && ParseUtil.isEOF(stmt, offset + 4)) {
                                    return TX_READ_ONLY;
                                }
                            }
                            return SYNTAX_ERROR;
                        case 'W':
                        case 'w':
                            if (horizontal) {
                                return SYNTAX_ERROR;
                            }
                            return isWrite(stmt, offset);
                        default:
                            return SYNTAX_ERROR;
                    }
                }
            }
        }
        return SYNTAX_ERROR;
    }

    private static boolean isOnly(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'L' || c2 == 'l') && (c3 == 'Y' || c3 == 'y')) {
                return true;
            }
        }
        return false;
    }

    private static int isWrite(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'I' || c2 == 'i') && (c3 == 'T' || c3 == 't') && (c4 == 'E' || c4 == 'e') && ParseUtil.isEOF(stmt, ++offset)) {
                return TX_READ_WRITE;
            }
        }
        return SYNTAX_ERROR;
    }

    // SET SESSION TRANSACTION ISOLATION LEVEL' '
    private static int level(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'V' || c2 == 'v') && (c3 == 'E' || c3 == 'e') &&
                    (c4 == 'L' || c4 == 'l') && ParseUtil.isSpace(stmt.charAt(++offset))) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'r':
                        case 'R':
                        case 's':
                        case 'S':
                            stmt = stmt.toUpperCase();
                            return parserLevel(stmt, offset, false, ' ');
                        default:
                            return SYNTAX_ERROR; //SET SESSION TRANSACTION ISOLATION LEVEL +OTHER SYNTAX
                    }
                }
            }
        }
        return SYNTAX_ERROR; //SET SESSION TRANSACTION ISOLATION L +OTHER SYNTAX
    }

    private static int parserLevel(String stmt, int offset, boolean checkApostrophe, char apostrophe) {
        switch (stmt.charAt(offset)) {
            case 'R':
            case 'r':
                return rCheck(stmt, offset, checkApostrophe, apostrophe);
            case 'S':
            case 's':
                return serializable(stmt, offset, checkApostrophe, apostrophe);
            default:
                return SYNTAX_ERROR; // Will not happen
        }
    }

    // SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE
    private static int serializable(String stmt, int offset, boolean checkApostrophe, char apostrophe) {
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
            char c11 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'R' || c2 == 'r') && (c3 == 'I' || c3 == 'i') &&
                    (c4 == 'A' || c4 == 'a') && (c5 == 'L' || c5 == 'l') && (c6 == 'I' || c6 == 'i') &&
                    (c7 == 'Z' || c7 == 'z') && (c8 == 'A' || c8 == 'a') && (c9 == 'B' || c9 == 'b') &&
                    (c10 == 'L' || c10 == 'l') && (c11 == 'E' || c11 == 'e')) {
                if (checkApostrophe && stmt.charAt(++offset) != apostrophe) {
                    return SYNTAX_ERROR;
                }
                return parseValueForExpected(stmt, offset, TX_SERIALIZABLE);
            }
        }
        return SYNTAX_ERROR;
    }

    // READ' '|REPEATABLE
    private static int rCheck(String stmt, int offset, boolean checkApostrophe, char apostrophe) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'E':
                case 'e':
                    return eCheck(stmt, offset, checkApostrophe, apostrophe);
                default:
                    return SYNTAX_ERROR; //SET SESSION TRANSACTION ISOLATION LEVEL R +OTHER
            }
        }
        return SYNTAX_ERROR; //SET SESSION TRANSACTION ISOLATION LEVEL R;
    }

    // READ' '|REPEATABLE
    private static int eCheck(String stmt, int offset, boolean checkApostrophe, char apostrophe) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'A':
                case 'a':
                    return aCheck(stmt, offset, checkApostrophe, apostrophe);
                case 'P':
                case 'p':
                    return pCheck(stmt, offset, checkApostrophe, apostrophe);
                default:
                    return SYNTAX_ERROR; //SET SESSION TRANSACTION ISOLATION LEVEL RE +OTHER
            }
        }
        return SYNTAX_ERROR; //SET SESSION TRANSACTION ISOLATION LEVEL RE;
    }

    // READ' '||READ-
    private static int aCheck(String stmt, int offset, boolean checkApostrophe, char apostrophe) {
        if ((stmt.length() > offset + 2) && (stmt.charAt(++offset) == 'D' || stmt.charAt(offset) == 'd')) {
            if (checkApostrophe) {
                if (stmt.charAt(++offset) != '-') {
                    return SYNTAX_ERROR;
                }
                offset++;
            } else if (!ParseUtil.isSpace(stmt.charAt(++offset))) {
                return SYNTAX_ERROR;
            } else {
                boolean find = false;
                while (!find && stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\r':
                        case '\n':
                        case '\t':
                            continue;
                        case 'C':
                        case 'c':
                        case 'U':
                        case 'u':
                            find = true;
                            break;
                        default:
                            return SYNTAX_ERROR; //SET SESSION TRANSACTION ISOLATION LEVEL READ +OTHER;
                    }
                }
            }
            switch (stmt.charAt(offset)) {
                case 'C':
                case 'c':
                    return committed(stmt, offset, checkApostrophe, apostrophe);
                case 'U':
                case 'u':
                    return uncommitted(stmt, offset, checkApostrophe, apostrophe);
                default:
                    return SYNTAX_ERROR;
            }
        }
        return SYNTAX_ERROR; //SET SESSION TRANSACTION ISOLATION LEVEL REA +OTHER;
    }

    // COMMITTED
    private static int committed(String stmt, int offset, boolean checkApostrophe, char apostrophe) {
        if (stmt.length() > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'M' || c2 == 'm') && (c3 == 'M' || c3 == 'm') &&
                    (c4 == 'I' || c4 == 'i') && (c5 == 'T' || c5 == 't') && (c6 == 'T' || c6 == 't') &&
                    (c7 == 'E' || c7 == 'e') && (c8 == 'D' || c8 == 'd')) {
                if (checkApostrophe && stmt.charAt(++offset) != apostrophe) {
                    return SYNTAX_ERROR;
                }
                return parseValueForExpected(stmt, offset, TX_READ_COMMITTED);
            }
        }
        return SYNTAX_ERROR;
    }

    // UNCOMMITTED
    private static int uncommitted(String stmt, int offset, boolean checkApostrophe, char apostrophe) {
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
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'C' || c2 == 'c') && (c3 == 'O' || c3 == 'o') &&
                    (c4 == 'M' || c4 == 'm') && (c5 == 'M' || c5 == 'm') && (c6 == 'I' || c6 == 'i') &&
                    (c7 == 'T' || c7 == 't') && (c8 == 'T' || c8 == 't') && (c9 == 'E' || c9 == 'e') &&
                    (c10 == 'D' || c10 == 'd')) {
                if (checkApostrophe && stmt.charAt(++offset) != apostrophe) {
                    return SYNTAX_ERROR;
                }
                return parseValueForExpected(stmt, offset, TX_READ_UNCOMMITTED);
            }
        }
        return SYNTAX_ERROR;
    }

    // REPEATABLE
    private static int pCheck(String stmt, int offset, boolean checkApostrophe, char apostrophe) {
        if (stmt.length() > offset + 8) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'A' || c2 == 'a') && (c3 == 'T' || c3 == 't') &&
                    (c4 == 'A' || c4 == 'a') && (c5 == 'B' || c5 == 'b') && (c6 == 'L' || c6 == 'l') &&
                    (c7 == 'E' || c7 == 'e')) {
                if (checkApostrophe) {
                    if (stmt.charAt(++offset) != '-') {
                        return SYNTAX_ERROR;
                    }
                    offset++;
                } else if (!ParseUtil.isSpace(stmt.charAt(++offset))) {
                    return SYNTAX_ERROR;
                } else {
                    boolean find = false;
                    while (!find && stmt.length() > ++offset) {
                        switch (stmt.charAt(offset)) {
                            case ' ':
                            case '\r':
                            case '\n':
                            case '\t':
                                continue;
                            case 'R':
                            case 'r':
                                find = true;
                                break;
                            default:
                                return SYNTAX_ERROR;
                        }
                    }
                }
                switch (stmt.charAt(offset)) {
                    case 'R':
                    case 'r':
                        return prCheck(stmt, offset, checkApostrophe, apostrophe);
                    default:
                        return SYNTAX_ERROR;
                }
            }
        }
        return SYNTAX_ERROR;
    }

    // READ
    private static int prCheck(String stmt, int offset, boolean checkApostrophe, char apostrophe) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'E' || c1 == 'e') && (c2 == 'A' || c2 == 'a') && (c3 == 'D' || c3 == 'd')) {
                if (checkApostrophe && stmt.charAt(++offset) != apostrophe) {
                    return SYNTAX_ERROR;
                }
                return parseValueForExpected(stmt, offset, TX_REPEATED_READ);
            }
        }
        return SYNTAX_ERROR;
    }

    private static boolean isIsolation(String stmt, int offset) {
        char c1 = stmt.charAt(++offset);
        char c2 = stmt.charAt(++offset);
        char c3 = stmt.charAt(++offset);
        char c4 = stmt.charAt(++offset);
        char c5 = stmt.charAt(++offset);
        char c6 = stmt.charAt(++offset);
        char c7 = stmt.charAt(++offset);
        char c8 = stmt.charAt(++offset);
        return (c1 == 'S' || c1 == 's') && (c2 == 'O' || c2 == 'o') && (c3 == 'L' || c3 == 'l') && (c4 == 'A' || c4 == 'a') &&
                (c5 == 'T' || c5 == 't') && (c6 == 'I' || c6 == 'i') && (c7 == 'O' || c7 == 'o') &&
                (c8 == 'N' || c8 == 'n');
    }

    private static boolean isSession(String stmt, int offset) {
        char c1 = stmt.charAt(++offset);
        char c2 = stmt.charAt(++offset);
        char c3 = stmt.charAt(++offset);
        char c4 = stmt.charAt(++offset);
        char c5 = stmt.charAt(++offset);
        char c6 = stmt.charAt(++offset);
        return (c1 == 'E' || c1 == 'e') && (c2 == 'S' || c2 == 's') && (c3 == 'S' || c3 == 's') && (c4 == 'I' || c4 == 'i') &&
                (c5 == 'O' || c5 == 'o') && (c6 == 'N' || c6 == 'n');
    }

    private static boolean isGlobal(String stmt, int offset) {
        char c1 = stmt.charAt(++offset);
        char c2 = stmt.charAt(++offset);
        char c3 = stmt.charAt(++offset);
        char c4 = stmt.charAt(++offset);
        char c5 = stmt.charAt(++offset);
        return (c1 == 'L' || c1 == 'l') && (c2 == 'O' || c2 == 'o') && (c3 == 'B' || c3 == 'b') && (c4 == 'A' || c4 == 'a') &&
                (c5 == 'L' || c5 == 'l');
    }

    private static int checkSystemVariables(String stmt) {
        //TODO:
        return OTHER;
    }

    private static int checkUserVariables(String stmt) {
        //TODO:
        return OTHER;
    }
}
