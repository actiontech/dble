/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.server.parser;

import com.oceanbase.obsharding_d.route.parser.util.ParseUtil;

import java.util.regex.Matcher;

/**
 * @author mycat
 */
public final class RwSplitServerParse extends AbstractServerParse {
    protected RwSplitServerParse() {
        super();
    }

    @Override
    public int parse(String stmt) {
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
                    rt = serverParseValidations.beginCheck(stmt, i);
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
                    rt = serverParseValidations.gCheck(stmt, i);
                    break;
                case 'I':
                case 'i':
                    rt = iCheck(stmt, i);
                    break;
                case 'M':
                case 'm':
                    rt = serverParseValidations.migrateCheck(stmt, i);
                    break;
                case 'O':
                case 'o':
                    rt = serverParseValidations.optimizeCheck(stmt, i);
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
                    rt = serverParseValidations.tCheck(stmt, i);
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
                    rt = serverParseValidations.helpCheck(stmt, i);
                    break;
                case 'L':
                case 'l':
                    rt = lCheck(stmt, i);
                    break;
                case 'X':
                case 'x':
                    rt = xCheck(stmt, i);
                    break;
                default:
                    break;
            }
            break;
        }
        return rt;
    }

    public static int isSetAutocommitSql(String sql) {
        Matcher matcher = AUTOCOMMIT_PATTERN.matcher(sql);
        if (matcher.matches()) {
            String value = matcher.group(4);
            if (value != null) {
                value = value.trim();
                if (value.equals("1") || value.toLowerCase().equalsIgnoreCase("on")) {
                    return 1;
                } else if (value.equals("0") || value.toLowerCase().equalsIgnoreCase("off")) {
                    return 0;
                }
            }
        }
        return -1;
    }

    private int xCheck(String stmt, int offset) {
        if (stmt.length() > offset + 1) {
            switch (stmt.charAt(++offset)) {
                case 'A':
                case 'a':
                    char charAfterOffset = stmt.charAt(++offset);
                    if (charAfterOffset == ' ' || charAfterOffset == '\t' || charAfterOffset == '\r' || charAfterOffset == '\n')
                        return serverParseValidations.xaCheck(stmt, offset);
                    break;
                default:
                    break;
            }
        }
        return OTHER;
    }

    // INSERT' ' | INSTALL '  '
    protected int iCheck(String stmt, int offset) {
        int type = OTHER;
        if (stmt.length() > offset + 2) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'N' || c1 == 'n') && (c2 == 'S' || c2 == 's')) {
                switch (c3) {
                    case 'E':
                    case 'e':
                        type = serverParseValidations.inseCheck(stmt, offset);
                        break;
                    case 'T':
                    case 't':
                        type = serverParseValidations.instCheck(stmt, offset);
                        break;
                    default:
                        break;
                }
            }
        }
        return type;
    }

    protected int rCheck(String stmt, int offset) {
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

    private int reCheck(String stmt, int offset) {
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
                    return serverParseValidations.release(stmt, offset);
                case 'v':
                case 'V':
                    return serverParseValidations.revoke(stmt, offset);
                default:
                    return OTHER;
            }
        }
        return OTHER;
    }

    private int rename(String stmt, int offset) {
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
    private int unCheck(String stmt, int offset) {
        int type = OTHER;
        switch (stmt.charAt(++offset)) {
            case 'L':
            case 'l':
                type = serverParseValidations.unlCheck(stmt, offset);
                break;
            case 'I':
            case 'i':
                type = serverParseValidations.uniCheck(stmt, offset);
                break;
            default:
                break;
        }
        return type;
    }

    // UPDATE' ' | USE' '
    protected int uCheck(String stmt, int offset) {
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

    protected int sCheck(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
                case 'A':
                case 'a':
                    return serverParseValidations.savepointCheck(stmt, offset);
                case 'E':
                case 'e':
                    return seCheck(stmt, offset);
                case 'H':
                case 'h':
                    return serverParseValidations.showCheck(stmt, offset);
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
    private int startCheck(String stmt, int offset) {
        if (stmt.length() > offset + 4) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'R' || c2 == 'r') &&
                    (c3 == 'T' || c3 == 't') &&
                    (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                return (new RwSplitServerParseStart()).parse(stmt, offset);
            }
        }
        return OTHER;
    }

    //create table/view/...
    private int createCheck(String stmt, int offset) {
        int len = stmt.length();
        if (len > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'E' || c2 == 'e') && (c3 == 'A' || c3 == 'a') && (c4 == 'T' || c4 == 't') &&
                    (c5 == 'E' || c5 == 'e')) {
                while (len > ++offset) {
                    if (ParseUtil.isSpace(stmt.charAt(offset))) {
                        continue;
                    }
                    char c6 = stmt.charAt(offset);
                    if (c6 == 'd' || c6 == 'D') {
                        return serverParseValidations.databaseCheck(stmt, offset);
                    } else if (c6 == 'v' || c6 == 'V') {
                        return serverParseValidations.viewCheck(stmt, offset, false);
                    } else if (c6 == 'o' || c6 == 'O') {
                        return orCheck(stmt, offset);
                    } else if (c6 == 't' || c6 == 'T') {
                        if (len > ++offset) {
                            char c7 = stmt.charAt(offset);
                            if (c7 == 'e' || c7 == 'E') {
                                return serverParseValidations.createTempTableCheck(stmt, offset);
                            }
                        }
                        return DDL;
                    }
                }
                return DDL;
            }
        }
        return OTHER;
    }

    //drop
    private int dropCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'R' || c1 == 'r') && (c2 == 'O' || c2 == 'o') && (c3 == 'P' || c3 == 'p') &&
                    (c4 == ' ' || c4 == '\t' || c4 == '\r' || c4 == '\n')) {
                while (stmt.length() > ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                        case '\t':
                        case '\r':
                        case '\n':
                            continue;
                        case 'V':
                        case 'v':
                            return dropViewCheck(stmt, offset);
                        case 'P':
                        case 'p':
                            return dropPrepareCheck(stmt, offset);
                        case 't':
                        case 'T':
                            return serverParseValidations.dropTableCheck(stmt, offset);
                        default:
                            return DDL;
                    }
                }
            }
        }
        return OTHER;
    }

    private int dropPrepareCheck(String stmt, int offset) {
        if (serverParseValidations.isPrepare(stmt, offset)) return SCRIPT_PREPARE;
        return DDL;
    }

    private int dropViewCheck(String stmt, int offset) {
        if (stmt.length() > offset + 3) {
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c2 == 'i' || c2 == 'I') &&
                    (c3 == 'e' || c3 == 'E') &&
                    (c4 == 'w' || c4 == 'W') &&
                    (c5 == ' ' || c5 == '\t' || c5 == '\r' || c5 == '\n')) {
                return DROP_VIEW;
            }
        }
        return DDL;
    }

    // delete or drop
    private int deleteOrdCheck(String stmt, int offset) {
        int sqlType;
        switch (stmt.charAt((offset + 1))) {
            case 'E':
            case 'e':
                sqlType = dCheck(stmt, offset);
                break;
            case 'R':
            case 'r':
                sqlType = dropCheck(stmt, offset);
                break;
            default:
                sqlType = OTHER;
        }
        return sqlType;
    }

    protected int cCheck(String stmt, int offset) {
        int sqlType;
        switch (stmt.charAt((offset + 1))) {
            case 'A':
            case 'a':
                sqlType = serverParseValidations.callCheck(stmt, offset);
                break;
            case 'H':
            case 'h':
                sqlType = serverParseValidations.checksumCheck(stmt, offset);
                break;
            case 'O':
            case 'o':
                sqlType = serverParseValidations.commitCheck(stmt, offset);
                break;
            case 'R':
            case 'r':
                sqlType = createCheck(stmt, offset);
                break;
            default:
                sqlType = OTHER;
        }
        return sqlType;
    }

    // ROLLBACK
    protected int rollbackCheck(String stmt, int offset) {
        int len = stmt.length();
        if (len > offset + 6) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'L' || c2 == 'l') &&
                    (c3 == 'B' || c3 == 'b') && (c4 == 'A' || c4 == 'a') &&
                    (c5 == 'C' || c5 == 'c') && (c6 == 'K' || c6 == 'k')) {
                char tmp;
                while (len > ++offset) {
                    tmp = stmt.charAt(offset);
                    if (ParseUtil.isSpace(tmp)) {
                        continue;
                    }
                    switch (tmp) {
                        case '/':
                            offset = ParseUtil.comment(stmt, offset);
                            break;
                        case 't':
                        case 'T':
                            return ROLLBACK_SAVEPOINT;
                        case 'w':
                        case 'W':
                            return serverParseValidations.rollbackWorkCheck(stmt, offset);
                        default:
                            break;
                    }
                }
                return ROLLBACK;
            }
        }
        return OTHER;
    }
}
