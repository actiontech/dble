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
public final class RwSplitServerParse extends ServerParse {
    private RwSplitServerParse() {
        super();
    }

    public static final int GRANT = 201;
    public static final int REVOKE = 202;
    public static final int INSTALL = 205;
    public static final int RENAME = 206;
    public static final int UNINSTALL = 207;
    public static final int START_TRANSACTION = 208;

    public static int parse(String stmt) {
        int length = stmt.length();
        //FIX BUG FOR SQL SUCH AS /XXXX/SQL
        int rt = OTHER;
        for (int i = 0; i < length; ++i) {
            switch (stmt.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
                case '/':
                    // such as /*!40101 SET character_set_client = @saved_cs_client
                    // */;
                    if (i == 0 && stmt.charAt(1) == '*' && stmt.charAt(2) == '!' && stmt.charAt(length - 2) == '*' &&
                            stmt.charAt(length - 1) == '/') {
                        return MYSQL_CMD_COMMENT;
                    }
                    //fall through
                case '#':
                    i = ParseUtil.comment(stmt, i);
                    if (i + 1 == length) {
                        return MYSQL_COMMENT;
                    }
                    continue;
                case '-':
                    i = ParseUtil.commentDoubleDash(stmt, i);
                    if (i + 1 == length) {
                        return MYSQL_COMMENT;
                    }
                    continue;
                case 'A':
                case 'a':
                    rt = aCheck(stmt, i);
                    break;
                case 'B':
                case 'b':
                    rt = beginCheck(stmt, i);
                    break;
                case 'C':
                case 'c':
                    rt = cCheck(stmt, i);
                    break;
                case 'D':
                case 'd':
                    rt = deleteOrdCheck(stmt, i);
                    break;
                case 'E':
                case 'e':
                    rt = eCheck(stmt, i);
                    break;
                case 'G':
                case 'g':
                    rt = gCheck(stmt, i);
                    break;
                case 'I':
                case 'i':
                    rt = iCheck(stmt, i);
                    break;
                case 'M':
                case 'm':
                    rt = migrateCheck(stmt, i);
                    break;
                case 'O':
                case 'o':
                    rt = optimizeCheck(stmt, i);
                    break;
                case 'P':
                case 'p':
                    rt = prepareCheck(stmt, i);
                    break;
                case 'R':
                case 'r':
                    rt = rCheck(stmt, i);
                    break;
                case 'S':
                case 's':
                    rt = sCheck(stmt, i);
                    break;
                case 'T':
                case 't':
                    rt = tCheck(stmt, i);
                    break;
                case 'U':
                case 'u':
                    rt = uCheck(stmt, i);
                    break;
                case 'K':
                case 'k':
                    rt = killCheck(stmt, i);
                    break;
                case 'H':
                case 'h':
                    rt = helpCheck(stmt, i);
                    break;
                case 'L':
                case 'l':
                    rt = lCheck(stmt, i);
                    break;
                default:
                    break;
            }
            break;
        }
        return rt;
    }

    // INSERT' ' | INSTALL '  '
    protected static int iCheck(String stmt, int offset) {
        int type = OTHER;
        if (stmt.length() > offset + 2) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'S' || c2 == 's')) {
                switch (c3) {
                    case 'E':
                    case 'e':
                        type = inseCheck(stmt, offset);
                        break;
                    case 'T':
                    case 't':
                        type = instCheck(stmt, offset);
                        break;
                    default:
                        break;
                }
            }
        }
        return type;
    }

    private static int instCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'L' || c2 == 'l') && (c3 == 'L' || c3 == 'l') && ParseUtil.isSpace(c4)) {
                return INSTALL;
            }
        }
        return OTHER;
    }

    private static int inseCheck(String stmt, int offset) {
        if (stmt.length() > offset + 2) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'T' || c2 == 't') && ParseUtil.isSpace(c3)) {
                return INSERT;
            }
        }
        return OTHER;
    }


    //grant
    private static int gCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') &&
                    (c2 == 'A' || c2 == 'a') &&
                    (c3 == 'N' || c3 == 'n') &&
                    (c4 == 'T' || c4 == 't') && ParseUtil.isSpace(c5)) {
                return GRANT;
            }
        }
        return OTHER;
    }

    protected static int rCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'E':
                case 'e':
                    return reCheck(stmt, offset);
                case 'O':
                case 'o':
                    return rollbackCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private static int reCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'N':
                case 'n':
                    return rename(stmt, offset);
                case 'P':
                case 'p':
                    return repCheck(stmt, offset);
                case 'l':
                case 'L':
                    return release(stmt, offset);
                case 'v':
                case 'V':
                    return revoke(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }


    //revoke
    private static int revoke(String stmt, int offset) {
        if (stmt.length() > offset + 5) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'o' || c1 == 'O') && (c2 == 'k' || c2 == 'K') && (c3 == 'e' || c3 == 'E') && ParseUtil.isSpace(c4)) {
                return REVOKE;
            }
        }
        return OTHER;
    }

    protected static int rename(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'M' || c2 == 'm') && (c3 == 'E' || c3 == 'e') &&
                    (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                return RENAME;
            }
        }
        return OTHER;
    }

    //UNLOCK | UNINSTALL
    private static int unCheck(String stmt, int offset) {
        int type = OTHER;
        switch (stmt.charAt(++offset)) {
            case 'L':
            case 'l':
                type = unlCheck(stmt, offset);
                break;
            case 'I':
            case 'i':
                type = uniCheck(stmt, offset);
                break;
            default:
                break;
        }
        return type;
    }

    private static int uniCheck(String stmt, int offset) {
        if (stmt.length() > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') &&
                    (c2 == 'S' || c2 == 's') &&
                    (c3 == 'T' || c3 == 't') &&
                    (c4 == 'A' || c4 == 'a') &&
                    (c5 == 'L' || c5 == 'l') &&
                    (c6 == 'L' || c6 == 'l') &&
                    ParseUtil.isSpace(c7)) {
                return UNINSTALL;
            }
        }
        return OTHER;
    }

    private static int unlCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') &&
                    (c2 == 'C' || c2 == 'c') &&
                    (c3 == 'K' || c3 == 'k') &&
                    ParseUtil.isSpace(c4)) {
                return UNLOCK;
            }
        }
        return OTHER;
    }

    // UPDATE' ' | USE' '
    protected static int uCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'P':
                case 'p':
                    if (stmt.length() > offset + 5) {
                        char c1 = stmt.charAt(++offset);
                        char c2 = stmt.charAt(++offset);
                        char c3 = stmt.charAt(++offset);
                        char c4 = stmt.charAt(++offset);
                        char c5 = stmt.charAt(++offset);
                        if ((c1 == 'D' || c1 == 'd') &&
                                (c2 == 'A' || c2 == 'a') &&
                                (c3 == 'T' || c3 == 't') &&
                                (c4 == 'E' || c4 == 'e') &&
                                (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                            return UPDATE;
                        }
                    }
                    break;
                case 'S':
                case 's':
                    if (stmt.length() > offset + 2) {
                        char c1 = stmt.charAt(++offset);
                        char c2 = stmt.charAt(++offset);
                        if ((c1 == 'E' || c1 == 'e') &&
                                (c2 == ' ' || c2 == '\t' || c2 == '\r' || c2 == '\n')) {
                            return (offset << 8) | USE;
                        }
                    }
                    break;
                case 'N':
                case 'n':
                    return unCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    protected static int sCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'A':
                case 'a':
                    return savepointCheck(stmt, offset);
                case 'E':
                case 'e':
                    return seCheck(stmt, offset);
                case 'H':
                case 'h':
                    return showCheck(stmt, offset);
                case 'T':
                case 't':
                    return startCheck(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    // START' '
    private static int startCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'R' || c2 == 'r') &&
                    (c3 == 'T' || c3 == 't') &&
                    (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                stmt = stmt.substring(offset).trim();
                return RwSplitServerParseStart.parse(stmt, 0);
            }
        }
        return OTHER;
    }
}
